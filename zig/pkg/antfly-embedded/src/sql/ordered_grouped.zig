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

//! One bounded sort per compatible ordered aggregate domain, not per group
//! or per percentile. Group summaries are sorted separately and merge-zipped
//! with their input segments; no group-sized row array survives a scan page.
const std = @import("std");
const operators = @import("operators.zig");
const binding = @import("aggregate_binding.zig");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
const ordered = @import("ordered_aggregate.zig");
const arrays = @import("array_value.zig");
const A = std.mem.Allocator;
const Datum = scalar.Datum;

pub const Collector = struct {
    grouped: *operators.Grouped,
    bound: *const binding.Bound,
    manager: *spill.Manager,
    sorts: []spill.Sort = &.{},
    representatives: []usize = &.{},
    summaries: ?spill.Sort = null,
    key_buffer: []Datum = &.{},
    prepared: bool = false,

    pub fn init(grouped: *operators.Grouped, bound: *const binding.Bound, manager: *spill.Manager, bytes: usize) !Collector {
        var self: Collector = .{ .grouped = grouped, .bound = bound, .manager = manager };
        if (bound.ordered.len == 0) return self;
        const a = manager.allocator();
        self.key_buffer = try a.alloc(Datum, bound.group_count + 1);
        errdefer a.free(self.key_buffer);
        self.sorts = try a.alloc(spill.Sort, bound.ordered_class_count);
        errdefer a.free(self.sorts);
        self.representatives = try a.alloc(usize, bound.ordered_class_count);
        errdefer a.free(self.representatives);
        var initialized: usize = 0;
        errdefer for (self.sorts[0..initialized]) |*sort| {
            const orders = sort.orders;
            sort.deinit();
            a.free(orders);
        };
        // Grouped state uses half the budget; ordered domains and summaries
        // divide the remainder, irrespective of group or request counts.
        const per_sort = bytes / (bound.ordered_class_count + 1);
        for (self.sorts, 0..) |*sort, class| {
            const orders = try a.alloc(operators.Order, bound.group_count + 1);
            @memset(orders, .{});
            for (bound.ordered, 0..) |plan, index| if (plan.sort_class == class) {
                orders[bound.group_count] = .{ .descending = plan.order.descending, .nulls_first = plan.order.nulls_first };
                self.representatives[class] = index;
                break;
            };
            sort.* = spill.Sort.init(a, manager, orders, per_sort);
            initialized += 1;
        }
        const orders = try a.alloc(operators.Order, bound.group_count);
        @memset(orders, .{});
        self.summaries = spill.Sort.init(a, manager, orders, per_sort);
        return self;
    }

    pub fn deinit(self: *Collector) void {
        const a = self.manager.allocator();
        for (self.sorts) |*sort| {
            const orders = sort.orders;
            sort.deinit();
            a.free(orders);
        }
        a.free(self.sorts);
        a.free(self.representatives);
        a.free(self.key_buffer);
        if (self.summaries) |*sort| {
            const orders = sort.orders;
            sort.deinit();
            a.free(orders);
        }
    }

    fn capture(self: *Collector, keys: []const Datum, inputs: []const Datum) !void {
        if (self.sorts.len == 0) return;
        @memcpy(self.key_buffer[0..keys.len], keys);
        for (self.sorts, self.representatives) |*sort, index| {
            const plan = self.bound.ordered[index];
            const value = inputs[plan.aggregate_index];
            if (!value.sql_null) {
                self.key_buffer[keys.len] = value;
                try sort.add(.{ .values = &.{}, .keys = self.key_buffer, .ordinal = sort.total });
            }
        }
    }

    pub fn add(self: *Collector, keys: []const Datum, inputs: []const Datum) !void {
        try self.grouped.add(keys, inputs);
        try self.capture(keys, inputs);
    }
    pub fn addGlobalBatch(self: *Collector, inputs: []const []const Datum) !void {
        try self.grouped.addGlobalBatch(inputs);
        for (inputs) |row| try self.capture(&.{}, row);
    }
    pub fn addGlobalColumns(self: *Collector, inputs: []const []const Datum, count: usize) !void {
        try self.addColumns(&.{}, inputs, count);
    }
    pub fn addColumns(self: *Collector, keys: []const []const Datum, inputs: []const []const Datum, count: usize) !void {
        if (keys.len == 0) try self.grouped.addGlobalColumns(inputs, count) else try self.grouped.addColumns(keys, inputs, count);
        if (self.sorts.len == 0) return;
        for (0..count) |row| {
            for (keys, self.key_buffer[0..keys.len]) |column, *key| key.* = column[row];
            for (self.sorts, self.representatives) |*sort, index| {
                const plan = self.bound.ordered[index];
                const value = inputs[plan.aggregate_index][row];
                if (!value.sql_null) {
                    self.key_buffer[keys.len] = value;
                    try sort.add(.{ .values = &.{}, .keys = self.key_buffer, .ordinal = sort.total });
                }
            }
        }
    }

    const Reader = struct {
        sort: *spill.Sort,
        keys: []const Datum,
        pub fn next(self: *@This(), a: A) !?Datum {
            const row = (try self.sort.next(a)) orelse return null;
            if (row.keys.len != self.keys.len + 1 or row.values.len != 0) return error.InvalidSqlSpill;
            for (row.keys[0..self.keys.len], self.keys) |actual, expected| {
                if (actual.sql_null != expected.sql_null) return error.InvalidSqlSpill;
                if (!actual.sql_null and (try scalar.compareDatums(actual, expected)) != .eq) return error.InvalidSqlSpill;
            }
            return row.keys[self.keys.len];
        }
    };
    fn fraction(value: Datum) !?f64 {
        if (value.sql_null) return null;
        if (value.array != null) return error.SqlTypeMismatch;
        return switch (value.value) {
            .float => |n| n,
            .integer => |n| @floatFromInt(n),
            else => error.SqlTypeMismatch,
        };
    }
    fn request(kind: binding.OrderedKind, value: Datum) !ordered.Request {
        return switch (kind) {
            .mode => .mode,
            .continuous => .{ .continuous = try fraction(value) },
            .discrete => .{ .discrete = try fraction(value) },
        };
    }

    pub fn nextResult(self: *Collector, context: anytype, a: A) !?operators.GroupResult {
        const summaries = if (self.summaries) |*sort| sort else return self.grouped.nextResult(a);
        if (!self.prepared) {
            var scratch = std.heap.ArenaAllocator.init(self.manager.allocator());
            defer scratch.deinit();
            while (true) {
                try self.manager.check();
                _ = scratch.reset(.retain_capacity);
                const group = (try self.grouped.nextResult(scratch.allocator())) orelse break;
                try summaries.add(.{ .keys = group.keys, .values = group.aggregates, .ordinal = group.ordinal });
            }
            self.prepared = true;
        }
        const summary = (try summaries.next(a)) orelse {
            // Counts and sorted payloads must agree, including empty groups.
            for (self.sorts) |*sort| if (try sort.next(a) != null) return error.InvalidSqlSpill;
            return null;
        };
        const width = summary.keys.len + summary.values.len;
        const cells = try a.alloc(Datum, width + self.bound.constant_count);
        @memcpy(cells[0..summary.keys.len], summary.keys);
        @memcpy(cells[summary.keys.len..width], summary.values);
        @memcpy(cells[width..], context.invocation_constants);
        for (self.sorts, 0..) |*sort, class| {
            var requests: std.ArrayList(ordered.Request) = .empty;
            const Slot = struct { aggregate: usize, start: usize, len: usize, array: ?*const arrays.Value, array_result: bool };
            var slots: std.ArrayList(Slot) = .empty;
            var count: ?usize = null;
            var array_targets: usize = 0;
            for (self.bound.ordered) |plan| {
                if (plan.sort_class != class) continue;
                const counted = summary.values[plan.aggregate_index];
                if (counted.sql_null or counted.value != .integer or counted.value.integer < 0) return error.InvalidSqlSpill;
                const n = std.math.cast(usize, counted.value.integer) orelse return error.InvalidSqlSpill;
                if (count) |prior| if (prior != n) return error.InvalidSqlSpill;
                count = n;
                const direct = if (plan.direct) |program| try context.evaluate(a, program, cells) else Datum{};
                const array_result = if (plan.direct) |program| program.output_type.kind == .array else false;
                const start = requests.items.len;
                if (direct.array) |array| {
                    if (array.elements.len > self.manager.array_limits.elements -| array_targets) return error.SqlProgramLimitExceeded;
                    array_targets += array.elements.len;
                    for (array.elements) |element| try requests.append(a, try request(plan.kind, element));
                } else try requests.append(a, try request(plan.kind, direct));
                try slots.append(a, .{ .aggregate = plan.aggregate_index, .start = start, .len = requests.items.len - start, .array = direct.array, .array_result = array_result });
            }
            var reader: Reader = .{ .sort = sort, .keys = summary.keys };
            const output = try ordered.State.finishSorted(self.manager, count orelse return error.InvalidSqlBackendResponse, &reader, a, requests.items);
            for (slots.items) |slot| {
                var value: Datum = if (slot.len != 0) output[slot.start] else .{};
                if (slot.array_result) {
                    value = .{};
                    if (slot.array) |source| if (count.? != 0) {
                        const array = try a.create(arrays.Value);
                        var element_type: arrays.ElementType = .float64;
                        for (self.bound.ordered) |plan| if (plan.aggregate_index == slot.aggregate and plan.kind == .discrete) {
                            element_type = try scalar.parameterElementType(self.bound.input.projections[plan.input].?.output_type);
                            break;
                        };
                        array.* = try arrays.Value.init(element_type, source.dimensions, output[slot.start..][0..slot.len], self.manager.array_limits);
                        value = Datum.typedArray(array);
                    };
                }
                cells[summary.keys.len + slot.aggregate] = value;
            }
        }
        return .{ .keys = summary.keys, .aggregates = cells[summary.keys.len..width], .ordinal = summary.ordinal };
    }
};

test "SQL grouped ordered streams spill once per domain and release every allocation fault" {
    const Fixture = struct {
        invocation_constants: []const Datum = &.{},
        fn checkpoint(_: *anyopaque) !void {}
        pub fn evaluate(_: @This(), a: A, program: scalar.Program, cells: []const Datum) !Datum {
            return @import("decision_eval.zig").evaluate(a, null, &program, cells, &.{});
        }
        fn run(a: A, disk: bool) !void {
            var compiled = try @import("compiler.zig").compile(a, "SELECT g,percentile_cont(0.5) WITHIN GROUP(ORDER BY x),percentile_disc(0.5) WITHIN GROUP(ORDER BY x),mode() WITHIN GROUP(ORDER BY x) FROM t GROUP BY g", .{});
            defer compiled.deinit();
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const table: @import("catalog.zig").Table = .{ .id = 1, .physical_name = "t", .schema_version = 1, .columns = &.{ .{ .name = "g", .path = "g", .type = .integer }, .{ .name = "x", .path = "x", .type = .integer } } };
            const bound = try binding.bind(arena.allocator(), table, compiled.statement.select, &.{});
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = if (disk) std.testing.io else .failing, .context = &marker, .checkpoint = checkpoint, .async_writes = false, .max_bytes = if (disk) 8 * 1024 * 1024 else 0 };
            defer manager.deinit();
            const grouped = try operators.Grouped.create(a, bound.specs, .{ .bytes = if (disk) 32768 else 1024 * 1024, .spill = &manager });
            defer grouped.deinit();
            var collector = try Collector.init(grouped, &bound, &manager, if (disk) 8192 else 1024 * 1024);
            defer collector.deinit();
            try std.testing.expectEqual(@as(usize, 1), collector.sorts.len);
            const groups: usize = if (disk) 128 else 3;
            // Interleaved keys force nonadjacent groups through the shared
            // sort. Two equal-frequency values make mode's tie rule visible.
            for (0..4) |i| for (0..groups) |g| {
                const value = Datum.json(.{ .integer = @intCast(i % 2 + 1) });
                try collector.add(&.{Datum.json(.{ .integer = @intCast(groups - g - 1) })}, &.{ value, value, value });
            };
            try std.testing.expectEqual(groups * 4, collector.sorts[0].total);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            for (0..groups) |g| {
                _ = scratch.reset(.retain_capacity);
                const result = (try collector.nextResult(@This(){}, scratch.allocator())).?;
                try std.testing.expectEqual(@as(i64, @intCast(g)), result.keys[0].value.integer);
                try std.testing.expectEqual(@as(f64, 1.5), result.aggregates[0].value.float);
                try std.testing.expectEqual(@as(i64, 1), result.aggregates[1].value.integer);
                try std.testing.expectEqual(@as(i64, 1), result.aggregates[2].value.integer);
            }
            try std.testing.expect(try collector.nextResult(@This(){}, scratch.allocator()) == null);
            if (disk) try std.testing.expect(manager.written_bytes != 0);
        }
        fn fault(a: A) !void {
            try run(a, false);
        }
    };
    try Fixture.run(std.testing.allocator, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.fault, .{});
}
