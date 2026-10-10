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

//! External continuation of a streaming hash set. Right counts are residual
//! membership/multiplicity; UNION's right markers mean already emitted. Thus
//! promotion never replays rows already delivered to the parent iterator.
const std = @import("std");
const scalar = @import("scalar.zig");
const operators = @import("operators.zig");
const spill = @import("spill.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;

pub const Kind = enum { @"union", intersect, except };

pub const State = struct {
    a: A,
    manager: *spill.Manager,
    kind: Kind,
    all: bool,
    orders: []operators.Order,
    sorted: spill.Sort,
    group: std.heap.ArenaAllocator,
    keys: []const Datum = &.{},
    pending: ?spill.Sort.RowLease = null,
    copies: usize = 0,
    groups: usize = 0,
    max_groups: usize,
    eof: bool = false,

    pub fn create(a: A, manager: *spill.Manager, kind: Kind, all: bool, width: usize, memory_bytes: usize, max_groups: usize) !*State {
        // UNION ALL is streaming and must never enter a deduplicating state.
        if (kind == .@"union" and all) return error.InvalidSqlBackendResponse;
        const self = try a.create(State);
        errdefer a.destroy(self);
        const orders = try a.alloc(operators.Order, width);
        errdefer a.free(orders);
        @memset(orders, .{});
        self.* = .{ .a = a, .manager = manager, .kind = kind, .all = all, .orders = orders, .sorted = spill.Sort.init(a, manager, orders, memory_bytes), .group = .init(a), .max_groups = max_groups };
        return self;
    }

    pub fn deinit(self: *State) void {
        if (self.pending) |row| row.release();
        self.sorted.deinit();
        self.group.deinit();
        self.a.free(self.orders);
        self.a.destroy(self);
    }

    pub fn add(self: *State, keys: []const Datum, right: bool, count: usize) !void {
        if (count == 0) return;
        const encoded = std.math.cast(i64, count) orelse return error.SqlProgramLimitExceeded;
        try self.sorted.add(.{ .values = &.{Datum.json(.{ .integer = encoded })}, .keys = keys, .ordinal = @intFromBool(!right) });
    }

    fn equal(left: []const Datum, right: spill.Sort.RowLease) !bool {
        for (left, 0..) |l, i| {
            const r = try right.keyCell(i);
            if (l.sql_null != r.sql_null) return false;
            if (!l.sql_null and try scalar.compareDatums(l, r) != .eq) return false;
        }
        return true;
    }

    fn pull(self: *State) !?spill.Sort.RowLease {
        if (self.pending) |row| {
            self.pending = null;
            return row;
        }
        return self.sorted.nextLeased();
    }

    fn accumulate(row: spill.Sort.RowLease, left: *usize, right: *usize) !void {
        const value = try row.cell(0);
        if (value.sql_null or value.value != .integer or value.value.integer <= 0 or row.row.ordinal > 1) return error.InvalidSqlSpill;
        const count = std.math.cast(usize, value.value.integer) orelse return error.InvalidSqlSpill;
        const target = if (row.row.ordinal == 0) right else left;
        target.* = std.math.add(usize, target.*, count) catch return error.SqlProgramLimitExceeded;
    }

    /// The caller finishes ingestion before delivery. Only one owned key and
    /// one lookahead row survive a pull, regardless of duplicate multiplicity.
    pub fn next(self: *State, a: A) !?[]const Datum {
        while (self.copies == 0) {
            if (self.eof) return null;
            try self.manager.checkpoint(self.manager.context);
            if (!self.group.reset(.{ .retain_with_limit = 16 * 1024 })) return error.OutOfMemory;
            const first = try self.pull() orelse {
                self.eof = true;
                return null;
            };
            var left: usize = 0;
            var right: usize = 0;
            {
                defer first.release();
                if (self.groups >= self.max_groups) return error.SqlProgramLimitExceeded;
                self.groups += 1;
                const keys = try self.group.allocator().alloc(Datum, self.orders.len);
                for (keys, 0..) |*out, i| out.* = try operators.cloneDatum(self.group.allocator(), try first.keyCell(i));
                self.keys = keys;
                try accumulate(first, &left, &right);
            }
            while (try self.pull()) |row| {
                var keep = false;
                defer if (!keep) row.release();
                try self.manager.checkpoint(self.manager.context);
                if (!try equal(self.keys, row)) {
                    self.pending = row;
                    keep = true;
                    break;
                }
                try accumulate(row, &left, &right);
            } else self.eof = true;
            self.copies = switch (self.kind) {
                .@"union" => @intFromBool(left != 0 and right == 0),
                .intersect => if (self.all) @min(left, right) else @intFromBool(left != 0 and right != 0),
                .except => if (self.all) left -| right else @intFromBool(left != 0 and right == 0),
            };
        }
        try self.manager.checkpoint(self.manager.context);
        const result = try a.alloc(Datum, self.keys.len);
        for (self.keys, result) |key, *out| out.* = try operators.cloneDatum(a, key);
        self.copies -= 1;
        return result;
    }
};

test "SQL external set continuation preserves residual counts NULLs and emitted markers" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    const a = std.testing.allocator;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    for ([_]struct { kind: Kind, all: bool, count: usize }{
        .{ .kind = .@"union", .all = false, .count = 1 },
        .{ .kind = .intersect, .all = false, .count = 1 },
        .{ .kind = .intersect, .all = true, .count = 2 },
        .{ .kind = .except, .all = false, .count = 1 },
        .{ .kind = .except, .all = true, .count = 2 },
    }) |case| {
        const state = try State.create(a, &manager, case.kind, case.all, 1, 32 * 1024, 100);
        defer state.deinit();
        try state.add(&.{.{}}, true, 2);
        try state.add(&.{.{}}, false, 3);
        try state.add(&.{Datum.json(.{ .integer = 7 })}, false, 1);
        var count: usize = 0;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        while (try state.next(arena.allocator())) |_| count += 1;
        try std.testing.expectEqual(case.count, count);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}

test "SQL external set partial ownership unwinds every allocation failure" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A) !void {
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .async_writes = false };
            defer manager.deinit();
            const state = try State.create(a, &manager, .except, true, 1, 32 * 1024, 100);
            defer state.deinit();
            try state.add(&.{Datum.json(.{ .string = "owned key" })}, false, 3);
            try state.add(&.{Datum.json(.{ .string = "owned key" })}, true, 1);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var count: usize = 0;
            while (try state.next(arena.allocator())) |_| count += 1;
            try std.testing.expectEqual(@as(usize, 2), count);
        }
    };
    try Fixture.run(std.testing.allocator);
    var no_resize = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{});
}

test "SQL external set leases retain complete array identities across disk blocks" {
    const arrays = @import("array_value.zig");
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    const a = std.testing.allocator;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    {
        const state = try State.create(a, &manager, .except, true, 1, 8 * 1024, 1000);
        defer state.deinit();
        for (0..200) |n| for ([_]i32{ -3, 2 }) |lower| {
            var array = try arrays.Value.init(.int64, &.{.{ .length = 2, .lower = lower }}, &.{ Datum.json(.{ .integer = @intCast(n) }), .{} }, .{});
            try state.add(&.{Datum.typedArray(&array)}, false, 2);
            try state.add(&.{Datum.typedArray(&array)}, true, 1);
        };
        var seen: [200][2]bool = @splat(@splat(false));
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count: usize = 0;
        while (try state.next(arena.allocator())) |row| {
            try std.testing.expectEqual(@as(usize, 1), row.len);
            try std.testing.expect(!row[0].sql_null);
            const array = row[0].array.?;
            try std.testing.expectEqual(arrays.ElementType.int64, array.element_type);
            try std.testing.expectEqual(@as(usize, 1), array.dimensions.len);
            try std.testing.expectEqual(@as(usize, 2), array.elements.len);
            try std.testing.expect(array.elements[1].sql_null);
            const n = std.math.cast(usize, array.elements[0].value.integer) orelse return error.UnexpectedSetValue;
            try std.testing.expect(n < seen.len);
            const bound: usize = if (array.dimensions[0].lower == -3) 0 else if (array.dimensions[0].lower == 2) 1 else return error.UnexpectedArrayBound;
            try std.testing.expect(!seen[n][bound]);
            seen[n][bound] = true;
            count += 1;
            _ = arena.reset(.{ .retain_with_limit = 16 * 1024 });
        }
        try std.testing.expectEqual(@as(usize, 400), count);
        try std.testing.expect(manager.written_bytes > 0);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}

test "SQL external set disk quota cancellation and group caps release all files" {
    const Hook = struct {
        canceled: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |cancel| {
        var hook: Hook = .{};
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &hook, .checkpoint = Hook.check, .max_bytes = if (cancel) 1024 * 1024 else 1, .async_writes = false };
        defer manager.deinit();
        {
            const state = try State.create(a, &manager, .intersect, true, 1, 8 * 1024, 10_000);
            defer state.deinit();
            var exhausted = false;
            for (0..2048) |i| {
                state.add(&.{Datum.json(.{ .integer = @intCast(i) })}, false, 1) catch |err| {
                    try std.testing.expect(!cancel);
                    try std.testing.expectEqual(error.SqlProgramLimitExceeded, err);
                    exhausted = true;
                    break;
                };
            }
            if (cancel) {
                try std.testing.expect(manager.written_bytes > 0);
                hook.canceled = true;
                try std.testing.expectError(error.Canceled, state.next(a));
            } else try std.testing.expect(exhausted);
        }
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    }
    var hook: Hook = .{};
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &hook, .checkpoint = Hook.check };
    defer manager.deinit();
    const state = try State.create(a, &manager, .except, false, 1, 8 * 1024, 0);
    defer state.deinit();
    try state.add(&.{Datum.json(.{ .integer = 1 })}, false, 1);
    try std.testing.expectError(error.SqlProgramLimitExceeded, state.next(a));
}
