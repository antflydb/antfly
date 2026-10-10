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

//! Shared ordered-set transition: one bounded sort and one streaming final
//! pass for compatible percentile/mode requests. SQL binding/activation is a
//! separate boundary; this operator does not claim source-case parity alone.
const std = @import("std");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const spill = @import("spill.zig");
const A = std.mem.Allocator;
const Datum = scalar.Datum;

pub const Request = union(enum) {
    mode,
    continuous: ?f64,
    discrete: ?f64,
};

pub const State = struct {
    sort: spill.Sort,
    count: usize = 0,
    finished: bool = false,
    failed: bool = false,

    pub fn init(manager: *spill.Manager, memory_bytes: usize, order: operators.Order) !State {
        const orders = try manager.allocator().alloc(operators.Order, 1);
        orders[0] = order;
        return .{ .sort = spill.Sort.init(manager.allocator(), manager, orders, memory_bytes) };
    }
    pub fn deinit(self: *State) void {
        const orders = self.sort.orders;
        const a = self.sort.manager.allocator();
        self.sort.deinit();
        a.free(orders);
        self.* = undefined;
    }
    pub fn add(self: *State, value: Datum) !void {
        if (self.finished or self.failed) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        // Ordered-set aggregates ignore SQL NULL, not logical JSON null.
        if (value.sql_null) return;
        if (self.count == std.math.maxInt(usize)) return error.SqlProgramLimitExceeded;
        // The sort key is the payload. Do not retain/serialize a second copy
        // of every potentially wide string/JSON/array value as row values.
        try self.sort.add(.{ .values = &.{}, .keys = &.{value}, .ordinal = self.count });
        self.count += 1;
    }

    const Event = struct { rank: usize, output: usize, upper: bool = false, blend: f64 = 0 };
    fn eventLess(_: void, left: Event, right: Event) bool {
        if (left.rank != right.rank) return left.rank < right.rank;
        // Lower samples must precede upper samples when rounding makes the
        // two ranks coincide. This also preserves repeated percentile slots.
        return @intFromBool(left.upper) < @intFromBool(right.upper);
    }
    fn number(value: Datum) !f64 {
        return switch (value.value) {
            .integer => |n| @floatFromInt(n),
            .float => |n| n,
            else => error.SqlTypeMismatch,
        };
    }
    fn retainBest(arena: *std.heap.ArenaAllocator, previous: Datum, length: usize, best: *Datum, best_count: *usize) !void {
        if (length <= best_count.*) return;
        _ = arena.reset(.retain_capacity);
        best.* = try operators.cloneDatum(arena.allocator(), previous);
        best_count.* = length;
    }

    /// Returned cells belong to the caller's result arena, never to a spill
    /// head or mutable transition buffer. Request admission is independent of
    /// group size. Finalization is single-use on errors.
    pub fn finish(self: *State, result_arena: A, requests: []const Request) ![]const Datum {
        if (self.finished or self.failed) return error.InvalidSqlBackendResponse;
        self.finished = true;
        const Reader = struct {
            sort: *spill.Sort,
            pub fn next(reader: *@This(), a: A) !?Datum {
                const row = (try reader.sort.next(a)) orelse return null;
                if (row.keys.len != 1 or row.values.len != 0) return error.InvalidSqlSpill;
                return row.keys[0];
            }
        };
        var reader: Reader = .{ .sort = &self.sort };
        return finishSorted(self.sort.manager, self.count, &reader, result_arena, requests);
    }

    /// Consume exactly one known-size group segment from an already sorted
    /// stream. The caller can share a global grouped sort without re-sorting
    /// each group or buffering its values. Even NULL-only requests drain the
    /// segment, so the next invocation starts at the next group's boundary.
    pub fn finishSorted(manager: *spill.Manager, count: usize, reader: anytype, result_arena: A, requests: []const Request) ![]const Datum {
        // Scalar slots are compiler-bounded; array fractions additionally
        // share the statement's logical array and resident-memory admission.
        const array_targets = requests.len -| 256;
        if (array_targets > manager.array_limits.elements or array_targets > manager.array_limits.bytes / (2 * @sizeOf(Event) + @sizeOf(Datum)) or requests.len > std.math.maxInt(usize) / 2) return error.SqlProgramLimitExceeded;
        const a = manager.allocator();
        const events = try a.alloc(Event, requests.len * 2);
        defer a.free(events);
        var length: usize = 0;
        var needs_mode = false;
        for (requests, 0..) |request, index| {
            if (request == .mode) {
                needs_mode = true;
                continue;
            }
            const fraction = (switch (request) {
                .continuous => |p| p,
                .discrete => |p| p,
                .mode => unreachable,
            }) orelse continue;
            // PostgreSQL validates direct arguments even for an empty group.
            if (!std.math.isFinite(fraction) or fraction < 0 or fraction > 1) return error.SqlNumericOutOfRange;
            if (count == 0) continue;
            if (request == .continuous) {
                const rank = fraction * @as(f64, @floatFromInt(count - 1));
                const lower: usize = @intFromFloat(@floor(rank));
                const upper: usize = @intFromFloat(@ceil(rank));
                events[length] = .{ .rank = @min(lower, count - 1), .output = index };
                length += 1;
                if (upper != lower) {
                    events[length] = .{ .rank = @min(upper, count - 1), .output = index, .upper = true, .blend = rank - @floor(rank) };
                    length += 1;
                }
            } else {
                const rank: usize = @intFromFloat(@ceil(fraction * @as(f64, @floatFromInt(count))));
                events[length] = .{ .rank = @min(rank -| 1, count - 1), .output = index };
                length += 1;
            }
        }
        const output = try result_arena.alloc(Datum, requests.len);
        @memset(output, .{});
        std.mem.sort(Event, events[0..length], {}, eventLess);
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var run = std.heap.ArenaAllocator.init(a);
        defer run.deinit();
        var best_arena = std.heap.ArenaAllocator.init(a);
        defer best_arena.deinit();
        var previous: ?Datum = null;
        var run_count: usize = 0;
        var best: Datum = .{};
        var best_count: usize = 0;
        var event: usize = 0;
        var rank: usize = 0;
        while (rank < count) : (rank += 1) {
            try manager.check();
            _ = scratch.reset(.retain_capacity);
            const value = (try reader.next(scratch.allocator())) orelse return error.InvalidSqlSpill;
            if (value.sql_null) return error.InvalidSqlSpill;
            if (needs_mode) {
                const different = if (previous) |prior| (try scalar.compareDatums(prior, value)) != .eq else true;
                if (different) {
                    if (previous) |prior| try retainBest(&best_arena, prior, run_count, &best, &best_count);
                    _ = run.reset(.retain_capacity);
                    previous = try operators.cloneDatum(run.allocator(), value);
                    run_count = 0;
                }
                run_count += 1;
            }
            while (event < length and events[event].rank == rank) : (event += 1) {
                const target = events[event];
                if (requests[target.output] == .discrete) {
                    output[target.output] = try operators.cloneDatum(result_arena, value);
                } else {
                    const current = try number(value);
                    const interpolated = if (target.upper) output[target.output].value.float + target.blend * (current - output[target.output].value.float) else current;
                    output[target.output] = Datum.json(.{ .float = interpolated });
                }
            }
        }
        if (previous) |prior| try retainBest(&best_arena, prior, run_count, &best, &best_count);
        const mode = if (best_count != 0) try operators.cloneDatum(result_arena, best) else Datum{};
        for (requests, output) |request, *value| if (request == .mode) {
            value.* = mode;
        };
        return output;
    }
};

test "SQL ordered-set array requests exceed scalar slot count within statement admission" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = .failing, .context = &marker, .checkpoint = Fixture.checkpoint, .max_bytes = 0, .async_writes = false };
    defer manager.deinit();
    // Reject a disabled spill before any failing Io operation or cleanup.
    try std.testing.expectError(error.SqlProgramLimitExceeded, manager.create());
    try std.testing.expect(manager.dir == null);
    var state = try State.init(&manager, 1024 * 1024, .{});
    defer state.deinit();
    try state.add(Datum.json(.{ .integer = 1 }));
    try state.add(Datum.json(.{ .integer = 3 }));
    var result = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer result.deinit();
    const requests: [257]Request = @splat(.{ .continuous = 0.25 });
    const output = try state.finish(result.allocator(), &requests);
    try std.testing.expectEqual(requests.len, output.len);
    for (output) |value| try std.testing.expectEqual(@as(f64, 1.5), value.value.float);
    // Disabling arrays must not disable compiler-bounded scalar aggregates.
    manager.array_limits = .{ .elements = 0, .bytes = 0 };
    var mode = try State.init(&manager, 1024 * 1024, .{});
    defer mode.deinit();
    try mode.add(Datum.json(.{ .integer = 7 }));
    const scalar_output = try mode.finish(result.allocator(), &.{.mode});
    try std.testing.expectEqual(@as(i64, 7), scalar_output[0].value.integer);
}

test "SQL ordered-set reducer shares one bounded sorted stream and preserves requested ranks" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    for ([_]usize{ 4096, 1024 * 1024 }) |budget| for ([_]bool{ false, true }) |descending| {
        var marker: u8 = 0;
        var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &marker, .checkpoint = Fixture.checkpoint, .async_writes = false, .max_bytes = 8 * 1024 * 1024 };
        defer manager.deinit();
        var state = try State.init(&manager, budget, .{ .descending = descending });
        defer state.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        // Every value has equal frequency: mode uses the first sorted value,
        // not physical input order. NULL contributes to neither count nor rank.
        for (0..512) |index| try state.add(Datum.json(.{ .integer = @intCast((511 - index) % 16) }));
        try state.add(.{});
        const output = try state.finish(arena.allocator(), &.{ .mode, .{ .continuous = 0.5 }, .{ .discrete = 0.5 }, .{ .continuous = 0.25 }, .{ .continuous = 0.75 }, .{ .continuous = null }, .{ .discrete = 0 }, .{ .discrete = 1 } });
        try std.testing.expectEqual(@as(i64, if (descending) 15 else 0), output[0].value.integer);
        try std.testing.expectEqual(@as(f64, 7.5), output[1].value.float);
        try std.testing.expectEqual(@as(i64, if (descending) 8 else 7), output[2].value.integer);
        try std.testing.expectEqual(@as(f64, if (descending) 11.25 else 3.75), output[3].value.float);
        try std.testing.expectEqual(@as(f64, if (descending) 3.75 else 11.25), output[4].value.float);
        try std.testing.expect(output[5].sql_null);
        try std.testing.expectEqual(@as(i64, if (descending) 15 else 0), output[6].value.integer);
        try std.testing.expectEqual(@as(i64, if (descending) 0 else 15), output[7].value.integer);
        if (budget == 4096) try std.testing.expect(manager.written_bytes != 0);
    };
}

test "SQL ordered-set reducer validates empty direct arguments and preserves exact discrete payloads" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &marker, .checkpoint = Fixture.checkpoint, .async_writes = false };
    defer manager.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var empty = try State.init(&manager, 4096, .{});
    defer empty.deinit();
    try std.testing.expectError(error.SqlNumericOutOfRange, empty.finish(arena.allocator(), &.{.{ .continuous = -1 }}));
    var state = try State.init(&manager, 4096, .{});
    defer state.deinit();
    try state.add(Datum.json(.{ .integer = 9007199254740993 }));
    try state.add(Datum.json(.{ .integer = 9007199254740995 }));
    const output = try state.finish(arena.allocator(), &.{.{ .discrete = 0.5 }});
    try std.testing.expectEqual(@as(i64, 9007199254740993), output[0].value.integer);
    try std.testing.expectError(error.InvalidSqlBackendResponse, state.add(.{}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, state.finish(arena.allocator(), &.{}));
}

test "SQL ordered-set reducer releases every allocation failure without borrowing sort heads" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(alloc: A) !void {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = alloc, .io = std.testing.io, .context = &marker, .checkpoint = checkpoint, .async_writes = false };
            defer manager.deinit();
            var state = try State.init(&manager, 1024 * 1024, .{});
            defer state.deinit();
            var result = std.heap.ArenaAllocator.init(alloc);
            defer result.deinit();
            for ([_][]const u8{ "z", "a", "a", "b", "b", "b" }) |text| try state.add(Datum.json(.{ .string = text }));
            const values = try state.finish(result.allocator(), &.{ .mode, .{ .discrete = 0.5 }, .{ .discrete = null }, .{ .discrete = 1 } });
            try std.testing.expectEqualStrings("b", values[0].value.string);
            try std.testing.expectEqualStrings("b", values[1].value.string);
            try std.testing.expect(values[2].sql_null);
            try std.testing.expectEqualStrings("z", values[3].value.string);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL ordered-set reducer shares cancellation and disk admission and distinguishes JSON null" {
    const Fixture = struct {
        canceled: bool = false,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
    };
    var fixture: Fixture = .{};
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.checkpoint, .async_writes = false, .max_bytes = 1 };
    defer manager.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var state = try State.init(&manager, 4096, .{});
    defer state.deinit();
    try state.add(Datum.json(.null));
    try state.add(.{});
    const values = try state.finish(arena.allocator(), &.{ .mode, .{ .discrete = 0.5 } });
    try std.testing.expect(!values[0].sql_null and values[0].value == .null);
    try std.testing.expect(!values[1].sql_null and values[1].value == .null);
    var canceled = try State.init(&manager, 4096, .{});
    defer canceled.deinit();
    try canceled.add(Datum.json(.{ .integer = 1 }));
    fixture.canceled = true;
    try std.testing.expectError(error.Canceled, canceled.finish(arena.allocator(), &.{.mode}));
    fixture.canceled = false;
    var quota = try State.init(&manager, 4096, .{});
    defer quota.deinit();
    var rejected = false;
    for (0..512) |index| {
        quota.add(Datum.json(.{ .integer = @intCast(index) })) catch |err| {
            try std.testing.expectEqual(error.SqlProgramLimitExceeded, err);
            rejected = true;
            break;
        };
    }
    try std.testing.expect(rejected);
    try std.testing.expectError(error.InvalidSqlBackendResponse, quota.add(.{}));
}

test "SQL ordered-set compatible requests share spill work instead of sorting once per percentile" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn written(requests: []const Request) !u64 {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &marker, .checkpoint = checkpoint, .async_writes = false, .max_bytes = 8 * 1024 * 1024 };
            defer manager.deinit();
            var state = try State.init(&manager, 4096, .{});
            defer state.deinit();
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            for (0..512) |index| try state.add(Datum.json(.{ .integer = @intCast((511 - index) % 16) }));
            _ = try state.finish(arena.allocator(), requests);
            return manager.written_bytes;
        }
    };
    const requests = [_]Request{ .mode, .{ .continuous = 0.25 }, .{ .continuous = 0.5 }, .{ .continuous = 0.75 } };
    const shared = try Fixture.written(&requests);
    var independent: u64 = 0;
    for (requests) |request| independent += try Fixture.written(&.{request});
    try std.testing.expect(shared > 0);
    try std.testing.expect(independent >= 3 * shared);
    std.debug.print("SQL ordered-set spill bytes: shared={d} independent={d}\n", .{ shared, independent });
}
