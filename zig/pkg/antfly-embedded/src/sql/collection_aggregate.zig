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

//! One bounded external stream per aggregate, shared across every group.
//! Sort keys own argument values whenever possible; payloads never duplicate
//! a wide value already retained as an ordering or DISTINCT key.
const std = @import("std");
const binding = @import("aggregate_binding.zig");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
const arrays = @import("array_value.zig");
const A = std.mem.Allocator;
const Datum = scalar.Datum;
const Location = union(enum) { key: usize, value: usize };

pub const Stream = struct {
    plan: binding.Collection,
    group_count: usize,
    sort: spill.Sort,
    keys: []Datum,
    payload: []Datum,
    locations: []Location,

    pub fn init(manager: *spill.Manager, plan: binding.Collection, groups: usize, bytes: usize) !Stream {
        const a = manager.allocator();
        const orders = try a.alloc(operators.Order, groups + plan.orders.len);
        errdefer a.free(orders);
        @memset(orders[0..groups], .{});
        for (plan.orders, orders[groups..]) |order, *out| out.* = .{ .descending = order.order.descending, .nulls_first = order.order.nulls_first };
        const keys = try a.alloc(Datum, orders.len);
        errdefer a.free(keys);
        const locations = try a.alloc(Location, plan.arguments.len);
        errdefer a.free(locations);
        var width: usize = 0;
        for (plan.arguments, locations) |argument, *location| {
            location.* = for (plan.orders, 0..) |order, index| {
                if (argument == order.input) break .{ .key = groups + index };
            } else blk: {
                const index = width;
                width += 1;
                break :blk .{ .value = index };
            };
        }
        return .{ .plan = plan, .group_count = groups, .sort = spill.Sort.init(a, manager, orders, bytes), .keys = keys, .payload = try a.alloc(Datum, width), .locations = locations };
    }
    pub fn deinit(self: *Stream) void {
        const a = self.sort.manager.allocator();
        const orders = self.sort.orders;
        self.sort.deinit();
        a.free(orders);
        a.free(self.keys);
        a.free(self.payload);
        a.free(self.locations);
        self.* = undefined;
    }
    fn captured(self: *const Stream, values: []const Datum, slot: usize) !Datum {
        const index = std.mem.indexOfScalar(usize, self.plan.captures, slot) orelse return error.InvalidSqlBackendResponse;
        return values[index];
    }
    pub fn add(self: *Stream, context: anytype, a: A, source: []const Datum, groups: []const Datum, programs: []const ?scalar.Program) !Datum {
        const values = try a.alloc(Datum, self.plan.captures.len);
        for (self.plan.captures, values, 0..) |slot, *value, index| value.* = (if (index < self.plan.constants.len) self.plan.constants[index] else null) orelse try context.evaluate(a, programs[slot].?, source);
        return self.addValues(values, groups);
    }
    pub fn addValues(self: *Stream, values: []const Datum, groups: []const Datum) !Datum {
        if (values.len != self.plan.captures.len) return error.InvalidSqlBackendResponse;
        const first = try self.captured(values, self.plan.arguments[0]);
        // Every argument/order expression above is evaluated exactly once,
        // even for a NULL string input. FILTER is applied by the caller first.
        if (self.plan.kind != .array and first.sql_null) return .{};
        if (groups.len != self.group_count) return error.InvalidSqlBackendResponse;
        @memcpy(self.keys[0..groups.len], groups);
        for (self.plan.orders, self.keys[groups.len..]) |order, *key| key.* = try self.captured(values, order.input);
        for (self.locations, self.plan.arguments) |location, argument| switch (location) {
            .key => {},
            .value => |index| self.payload[index] = try self.captured(values, argument),
        };
        try self.sort.add(.{ .keys = self.keys, .values = self.payload, .ordinal = self.sort.total });
        return Datum.json(.{ .integer = 1 });
    }
    fn payloadArgument(self: *const Stream, row: operators.Row, index: usize) !Datum {
        if (row.keys.len != self.keys.len or row.values.len != self.payload.len) return error.InvalidSqlSpill;
        return switch (self.locations[index]) {
            .key => |k| row.keys[k],
            .value => |k| row.values[k],
        };
    }
    fn sameValue(left: Datum, right: Datum) !bool {
        if (left.sql_null != right.sql_null) return false;
        return left.sql_null or (try scalar.compareDatums(left, right)) == .eq;
    }
    pub fn finish(self: *Stream, a: A, groups: []const Datum, count: usize) !Datum {
        var scratch = std.heap.ArenaAllocator.init(self.sort.manager.allocator());
        defer scratch.deinit();
        var previous = std.heap.ArenaAllocator.init(self.sort.manager.allocator());
        defer previous.deinit();
        var prior: ?[]const Datum = null;
        var text: std.ArrayList(u8) = .empty;
        defer text.deinit(a);
        var elements: std.ArrayList(Datum) = .empty;
        defer elements.deinit(a);
        var dimensions: ?[]const arrays.Dimension = null;
        var accepted: usize = 0;
        var logical_bytes: usize = 0;
        var reducer: ?operators.Aggregate = if (self.plan.kind == .reduce) try operators.Aggregate.initTyped(self.sort.manager.allocator(), self.plan.reducer.kind, self.plan.reducer.input_type, self.plan.reducer.input_element) else null;
        defer if (reducer) |*value| value.deinit();
        for (0..count) |_| {
            try self.sort.manager.check();
            _ = scratch.reset(.retain_capacity);
            const row = (try self.sort.next(scratch.allocator())) orelse return error.InvalidSqlSpill;
            for (row.keys[0..self.group_count], groups) |actual, expected| if (!try sameValue(actual, expected)) return error.InvalidSqlSpill;
            const arguments = try scratch.allocator().alloc(Datum, self.plan.arguments.len);
            for (arguments, 0..) |*value, index| value.* = try self.payloadArgument(row, index);
            if (self.plan.distinct) {
                if (prior) |values| {
                    var duplicate = true;
                    for (values, arguments) |left, right| if (!try sameValue(left, right)) {
                        duplicate = false;
                        break;
                    };
                    if (duplicate) continue;
                }
                _ = previous.reset(.retain_capacity);
                const values = try previous.allocator().alloc(Datum, arguments.len);
                for (arguments, values) |value, *copy| copy.* = try operators.cloneDatum(previous.allocator(), value);
                prior = values;
            }
            switch (self.plan.kind) {
                .string => {
                    if (arguments[0].sql_null or arguments[0].value != .string) return error.InvalidSqlSpill;
                    const value = arguments[0].value.string;
                    const delimiter: []const u8 = if (arguments[1].sql_null or accepted == 0) "" else if (arguments[1].value == .string) arguments[1].value.string else return error.InvalidSqlSpill;
                    const maximum = self.sort.manager.max_record_bytes;
                    if (delimiter.len > maximum -| text.items.len or value.len > maximum -| (text.items.len +| delimiter.len)) return error.SqlProgramLimitExceeded;
                    try text.appendSlice(a, delimiter);
                    try text.appendSlice(a, value);
                },
                .array => {
                    const value = arguments[0];
                    if (self.plan.input_array) {
                        if (value.sql_null) return error.SqlArrayAggregateNull;
                        const array = value.array orelse return error.InvalidSqlSpill;
                        if (array.cardinality() == 0) return error.SqlArrayAggregateEmpty;
                        if (array.dimensions.len == 6) return error.SqlProgramLimitExceeded;
                        if (dimensions) |shape| {
                            if (shape.len != array.dimensions.len) return error.SqlArrayAggregateDimensions;
                            for (shape, array.dimensions) |left, right| if (!std.meta.eql(left, right)) return error.SqlArrayAggregateDimensions;
                        } else dimensions = try a.dupe(arrays.Dimension, array.dimensions);
                        if (array.cardinality() > self.sort.manager.array_limits.elements -| elements.items.len) return error.SqlProgramLimitExceeded;
                        for (0..array.cardinality()) |index| try appendElement(self, a, &elements, &logical_bytes, array.flatCell(index));
                    } else try appendElement(self, a, &elements, &logical_bytes, value);
                },
                .reduce => try reducer.?.update(arguments[0]),
            }
            accepted += 1;
        }
        if (self.plan.kind == .reduce) return reducer.?.finishOutput(a);
        if (accepted == 0) return .{};
        if (self.plan.kind == .string) return Datum.json(.{ .string = try text.toOwnedSlice(a) });
        const shape = try a.alloc(arrays.Dimension, 1 + if (dimensions) |inner| inner.len else @as(usize, 0));
        shape[0] = .{ .length = std.math.cast(u32, accepted) orelse return error.SqlProgramLimitExceeded };
        if (dimensions) |inner| @memcpy(shape[1..], inner);
        const array = try a.create(arrays.Value);
        array.* = try arrays.Value.init(self.plan.element_type orelse return error.InvalidSqlBackendResponse, shape, try elements.toOwnedSlice(a), self.sort.manager.array_limits);
        return Datum.typedArray(array);
    }
    fn appendElement(self: *Stream, a: A, elements: *std.ArrayList(Datum), bytes: *usize, value: Datum) !void {
        if (elements.items.len == self.sort.manager.array_limits.elements) return error.SqlProgramLimitExceeded;
        const width = try operators.datumBytes(value);
        if (width > self.sort.manager.array_limits.bytes -| bytes.*) return error.SqlProgramLimitExceeded;
        const retained = try operators.cloneDatum(a, value);
        try elements.append(a, retained);
        bytes.* += width;
    }
};

test "SQL collection aggregates avoid duplicate wide spill payloads benchmark" {
    const Metric = struct { bytes: u64, digest: [32]u8 };
    const Run = struct {
        fn check(_: *anyopaque) !void {}
        fn sample(a: A, width: usize, duplicate_payload: bool) !Metric {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .async_writes = false, .compression = .none, .max_bytes = 32 * 1024 * 1024 };
            defer manager.deinit();
            const plan: binding.Collection = .{ .aggregate_index = 0, .kind = .string, .arguments = &.{ 0, 1 }, .orders = &.{ .{ .input = 0, .order = .{} }, .{ .input = 1, .order = .{} } }, .captures = &.{ 0, 1 }, .distinct = true };
            var stream = try Stream.init(&manager, plan, 0, 32 * 1024);
            defer stream.deinit();
            var baseline = spill.Sort.init(a, &manager, stream.sort.orders, 32 * 1024);
            defer baseline.deinit();
            var expected = std.crypto.hash.sha2.Sha256.init(.{});
            const rows = 512;
            for (0..rows) |index| {
                var storage: [1024]u8 = undefined;
                @memset(storage[0..width], 'x');
                _ = try std.fmt.bufPrint(storage[0..8], "{x:0>8}", .{index});
                const text = storage[0..width];
                if (index != 0) expected.update(":");
                expected.update(text);
                const values = [_]Datum{ Datum.json(.{ .string = text }), Datum.json(.{ .string = ":" }) };
                if (duplicate_payload) try baseline.add(.{ .keys = &values, .values = &values, .ordinal = index }) else _ = try stream.addValues(&values, &.{});
            }
            var result_arena = std.heap.ArenaAllocator.init(a);
            defer result_arena.deinit();
            var actual = std.crypto.hash.sha2.Sha256.init(.{});
            if (duplicate_payload) {
                var count: usize = 0;
                while (try baseline.next(result_arena.allocator())) |row| {
                    if (count != 0) actual.update(":");
                    actual.update(row.values[0].value.string);
                    count += 1;
                    _ = result_arena.reset(.retain_capacity);
                }
                try std.testing.expectEqual(@as(usize, rows), count);
            } else {
                try std.testing.expectEqual(@as(usize, 0), stream.payload.len);
                const value = try stream.finish(result_arena.allocator(), &.{}, rows);
                actual.update(value.value.string);
            }
            const digest = actual.finalResult();
            try std.testing.expectEqualSlices(u8, &expected.finalResult(), &digest);
            return .{ .bytes = manager.written_bytes, .digest = digest };
        }
    };
    for ([_]usize{ 128, 1024 }) |width| {
        const baseline = try Run.sample(std.testing.allocator, width, true);
        const shared = try Run.sample(std.testing.allocator, width, false);
        try std.testing.expect(shared.bytes != 0 and shared.bytes < baseline.bytes);
        try std.testing.expectEqualSlices(u8, &baseline.digest, &shared.digest);
        std.debug.print("collection spill payload component rows=512 value_bytes={d} duplicated_written_bytes={d} shared_written_bytes={d} preparation_and_ranking_excluded=true\n", .{ width, baseline.bytes, shared.bytes });
    }
}

test "SQL collection aggregates cancellation releases captured streams" {
    const Control = struct {
        cancelled: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.cancelled) return error.Cancelled;
        }
    };
    var control: Control = .{};
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &control, .checkpoint = Control.check, .max_bytes = 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    const plan: binding.Collection = .{ .aggregate_index = 0, .kind = .string, .arguments = &.{ 0, 1 }, .orders = &.{.{ .input = 0, .order = .{} }}, .captures = &.{ 0, 1 }, .distinct = false };
    var stream = try Stream.init(&manager, plan, 0, 4096);
    defer stream.deinit();
    for (0..32) |_| _ = try stream.addValues(&.{ Datum.json(.{ .string = "abc" }), Datum.json(.{ .string = "," }) }, &.{});
    control.cancelled = true;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.Cancelled, stream.finish(arena.allocator(), &.{}, 32));
}

test "SQL collection aggregates preserve volatile argument occurrences and ordering reuse" {
    const Context = struct {
        calls: usize = 0,
        pub fn evaluate(self: *@This(), a: A, _: scalar.Program, _: []const Datum) !Datum {
            self.calls += 1;
            return Datum.json(.{ .string = try std.fmt.allocPrint(a, "{d}", .{self.calls}) });
        }
        fn check(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compile(a, "SELECT string_agg(gen_random_uuid()::text,gen_random_uuid()::text ORDER BY gen_random_uuid()::text)", .{});
    defer compiled.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const bound = try binding.bind(arena.allocator(), null, compiled.statement.select, &.{});
    try std.testing.expect(bound.collection_rowwise);
    const plan = bound.collections[0];
    try std.testing.expectEqual(@as(usize, 2), plan.captures.len);
    try std.testing.expect(plan.arguments[0] != plan.arguments[1]);
    try std.testing.expectEqual(plan.arguments[0], plan.orders[0].input);
    try std.testing.expect(plan.constants[0] == null and plan.constants[1] == null);
    var context: Context = .{};
    var manager: spill.Manager = .{ .alloc = a, .io = .failing, .context = &context, .checkpoint = Context.check, .max_bytes = 0, .async_writes = false };
    defer manager.deinit();
    var stream = try Stream.init(&manager, plan, 0, 1024 * 1024);
    defer stream.deinit();
    for (0..3) |_| _ = try stream.add(&context, arena.allocator(), &.{}, &.{}, bound.input.projections);
    try std.testing.expectEqual(@as(usize, 6), context.calls);
    const result = try stream.finish(arena.allocator(), &.{}, 3);
    try std.testing.expectEqualStrings("14365", result.value.string);
}
