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

//! Immutable statement-local replay. Small inputs stay in admitted memory;
//! larger inputs spill to typed sequential blocks, without a per-row directory.
//! Independent readers reuse one captured input, never reopen live storage.
const std = @import("std");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
const Budget = @import("memory_budget.zig");
const A = std.mem.Allocator;
const Datum = scalar.Datum;
const Count = @import("catalog.zig").Count;

pub const Replay = struct {
    a: A,
    width: usize,
    memory: Budget,
    arena: std.heap.ArenaAllocator,
    rows: std.ArrayList([]const Datum) = .empty,
    weights: std.ArrayList(Count) = .empty,
    manager: ?*spill.Manager,
    disk: ?spill.Sequential = null,
    count: usize = 0,
    readers: usize = 0,
    finished: bool = false,
    failed: bool = false,
    weighted: bool = false,

    pub fn create(a: A, width: usize, memory_bytes: usize, manager: ?*spill.Manager) !*Replay {
        if (width > 1024) return error.SqlProgramLimitExceeded;
        const self = try a.create(Replay);
        self.* = .{ .a = a, .width = width, .memory = .{ .backing = a, .limit = memory_bytes }, .arena = undefined, .manager = manager };
        self.arena = .init(self.memory.allocator());
        return self;
    }

    pub fn deinit(self: *Replay) void {
        std.debug.assert(self.readers == 0);
        if (self.disk) |*run| run.close();
        self.rows.deinit(self.memory.allocator());
        self.weights.deinit(self.memory.allocator());
        self.arena.deinit();
        std.debug.assert(self.memory.live == 0);
        const a = self.a;
        a.destroy(self);
    }

    fn appendMemory(self: *Replay, values: []const Datum, weight: Count) !void {
        try self.rows.ensureUnusedCapacity(self.memory.allocator(), 1);
        try self.weights.ensureUnusedCapacity(self.memory.allocator(), 1);
        const a = self.arena.allocator();
        const row = try a.alloc(Datum, values.len);
        for (values, row) |value, *out| out.* = try operators.cloneDatum(a, value);
        const owned_weight = try weight.clone(a);
        self.rows.appendAssumeCapacity(row);
        self.weights.appendAssumeCapacity(owned_weight);
    }

    fn startSpill(self: *Replay) !void {
        const manager = self.manager orelse return error.SqlProgramLimitExceeded;
        self.disk = try spill.Sequential.init(manager, @max(128, manager.buffer_bytes *| 2));
        for (self.rows.items, self.weights.items) |row, weight| try self.appendDisk(row, weight);
        self.rows.clearAndFree(self.memory.allocator());
        self.weights.clearAndFree(self.memory.allocator());
        _ = self.arena.reset(.free_all);
        std.debug.assert(self.memory.live == 0);
    }

    pub fn append(self: *Replay, values: []const Datum) !void {
        return self.appendWeighted(values, 1);
    }

    pub fn appendWeighted(self: *Replay, values: []const Datum, weight: u64) !void {
        return self.appendCount(values, Count.init(weight));
    }
    fn appendDisk(self: *Replay, values: []const Datum, weight: Count) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const keys: []const Datum = if (weight.exact() == null) &.{Datum.json(.{ .string = try weight.text(arena.allocator()) })} else &.{};
        _ = try self.disk.?.append(.{ .values = values, .keys = keys, .ordinal = weight.exact() orelse 0 }, spill.none);
    }
    pub fn appendCount(self: *Replay, values: []const Datum, weight: Count) !void {
        if (weight.cap(1) == 0) return error.InvalidSqlBackendResponse;
        if (self.failed or self.finished or values.len != self.width) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (self.count == std.math.maxInt(usize)) return error.SqlProgramLimitExceeded;
        if (self.disk == null) self.appendMemory(values, weight) catch |err| {
            if (err != error.OutOfMemory or !self.memory.exhausted) return err;
            try self.startSpill();
        };
        if (self.disk != null) try self.appendDisk(values, weight);
        self.weighted = self.weighted or !weight.isOne();
        self.count += 1;
    }

    pub fn finish(self: *Replay) !void {
        if (self.failed) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (self.disk) |*run| try run.seal();
        self.finished = true;
    }

    pub fn openReader(self: *Replay) !*Reader {
        if (!self.finished or self.failed) return error.InvalidSqlBackendResponse;
        const reader = try self.a.create(Reader);
        errdefer self.a.destroy(reader);
        reader.* = .{ .owner = self, .count_arena = .init(self.a), .disk = if (self.disk) |*run| try run.openReader() else null };
        self.readers += 1;
        return reader;
    }

    pub const Reader = struct {
        owner: *Replay,
        disk: ?spill.Sequential.ReplayReader,
        index: usize = 0,
        remaining: Count = Count.init(0),
        count_arena: std.heap.ArenaAllocator,
        remaining_owned: bool = false,
        current: []const Datum = &.{},

        pub const Weighted = struct { values: []const Datum, weight: u64, count: Count };
        pub fn nextWeighted(self: *Reader) !?Weighted {
            if (self.remaining.cap(1) != 0) return error.InvalidSqlBackendResponse;
            if (self.index == self.owner.count) return null;
            _ = self.count_arena.reset(.retain_capacity);
            self.remaining_owned = false;
            const result: Weighted = if (self.disk) |*run| blk: {
                const record = (try run.readCurrent(self.index)).row;
                const count = if (record.ordinal != 0 and record.keys.len == 0) Count.init(record.ordinal) else blk_count: {
                    if (record.ordinal != 0 or record.keys.len != 1 or record.keys[0].sql_null or record.keys[0].value != .string) return error.InvalidSqlSpill;
                    break :blk_count try Count.parse(self.count_arena.allocator(), record.keys[0].value.string);
                };
                break :blk .{ .values = record.values, .weight = count.cap(std.math.maxInt(u64)), .count = count };
            } else .{ .values = self.owner.rows.items[self.index], .weight = self.owner.weights.items[self.index].cap(std.math.maxInt(u64)), .count = self.owner.weights.items[self.index] };
            if (result.weight == 0 or result.values.len != self.owner.width) return error.InvalidSqlBackendResponse;
            self.index += 1;
            return result;
        }

        /// A borrowed row survives other readers, but not this reader's next
        /// read or close. Retaining operators copy at their boundary.
        pub fn next(self: *Reader) !?[]const Datum {
            if (self.remaining.cap(1) == 0) {
                const bag = try self.nextWeighted() orelse return null;
                self.current = bag.values;
                self.remaining = bag.count;
            }
            try self.consume(1);
            return self.current;
        }

        fn consume(self: *Reader, amount: u64) !void {
            if (self.remaining.exact()) |small| {
                self.remaining = Count.init(small - amount);
                return;
            }
            // Private mutable copy: expansion never allocates another big count
            // per logical row or mutates the replay's immutable shared weights.
            if (!self.remaining_owned) {
                self.remaining = try self.remaining.clone(self.count_arena.allocator());
                self.remaining_owned = true;
            }
            const big = @constCast(self.remaining.big.?);
            var limbs: [std.math.big.int.calcLimbLen(@as(u64, std.math.maxInt(u64)))]std.math.big.Limb = undefined;
            const subtrahend = std.math.big.int.Mutable.init(&limbs, amount);
            var mutable = big.toMutable();
            mutable.sub(big.toConst(), subtrahend.toConst());
            big.setMetadata(mutable.positive, mutable.len);
            if (big.toInt(u64)) |small| {
                self.remaining = Count.init(small);
                self.remaining_owned = false;
            } else |_| {}
        }

        /// Borrow native columns until this reader loads its next block.
        pub fn nextBatch(self: *Reader, a: A, maximum: usize) !@import("execution_batch.zig").Batch {
            if (maximum == 0) return error.InvalidSqlLimit;
            if (self.owner.weighted) {
                // Plain batch consumers observe logical rows. Clone once per
                // physical bag before another read can replace its block, then
                // share that owned row within the bounded expanded prefix.
                var rows: std.ArrayList([]const Datum) = .empty;
                while (rows.items.len < maximum) {
                    if (self.remaining.cap(1) == 0) {
                        const bag = try self.nextWeighted() orelse break;
                        self.current = bag.values;
                        self.remaining = bag.count;
                    }
                    const row = try a.alloc(Datum, self.current.len);
                    for (self.current, row) |value, *out| out.* = try operators.cloneDatum(a, value);
                    const count: usize = @intCast(self.remaining.cap(maximum - rows.items.len));
                    try rows.appendNTimes(a, row, count);
                    try self.consume(count);
                }
                return .{ .rows = rows.items };
            }
            if (self.index == self.owner.count) return .{ .rows = &.{} };
            if (self.disk) |*run| {
                const batch = try run.readBatchCurrent(a, self.index, maximum);
                self.index = @intCast(batch.following);
                return batch.values;
            }
            const count = @min(maximum, self.owner.count - self.index);
            const rows = self.owner.rows.items[self.index..][0..count];
            self.index += count;
            return .{ .rows = rows };
        }

        pub fn rewind(self: *Reader) void {
            self.index = 0;
            self.remaining = Count.init(0);
            self.current = &.{};
        }

        pub fn close(self: *Reader) void {
            if (self.disk) |*run| run.deinit();
            self.count_arena.deinit();
            self.owner.readers -= 1;
            const a = self.owner.a;
            a.destroy(self);
        }
    };
};

test "SQL replay rows bound resident capacity and independently replay typed blocks" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    const replay = try Replay.create(a, 3, 1024, &manager);
    defer replay.deinit();
    for (0..2048) |i| try replay.append(&.{ Datum.json(.{ .integer = @intCast(i) }), .{}, Datum.json(.null) });
    try replay.finish();
    try std.testing.expect(replay.disk != null);
    try std.testing.expect(replay.memory.peak <= 1024);
    try std.testing.expectEqual(@as(usize, 0), replay.memory.live);
    try std.testing.expectEqual(@as(usize, 1), manager.files);
    const first = try replay.openReader();
    defer first.close();
    const second = try replay.openReader();
    defer second.close();
    const borrowed = (try first.next()).?;
    for (0..2048) |i| {
        const row = (try second.next()).?;
        try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].value.integer);
        try std.testing.expect(row[1].sql_null);
        try std.testing.expect(!row[2].sql_null and row[2].value == .null);
    }
    try std.testing.expectEqual(@as(i64, 0), borrowed[0].value.integer);
    for (1..2048) |i| try std.testing.expectEqual(@as(i64, @intCast(i)), (try first.next()).?[0].value.integer);
    try std.testing.expect((try first.next()) == null);
    first.rewind();
    try std.testing.expectEqual(@as(i64, 0), (try first.next()).?[0].value.integer);
    try std.testing.expectError(error.InvalidSqlBackendResponse, replay.append(&.{ .{}, .{}, .{} }));
    try std.testing.expectError(error.InvalidSqlSpill, replay.disk.?.append(.{ .values = &.{}, .keys = &.{}, .ordinal = 0 }, spill.none));
}

test "SQL replay rows preserve arrays and unwind every memory and spill allocation fault" {
    const Faults = struct {
        fn run(backing: A, spill_bytes: bool) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const a: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            const Hook = struct {
                fn check(_: *anyopaque) !void {}
            };
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            {
                const replay = try Replay.create(a, 1, if (spill_bytes) 1 else 64 * 1024, if (spill_bytes) &manager else null);
                defer replay.deinit();
                const arrays = @import("array_value.zig");
                const value = try arrays.Value.init(.int64, &.{.{ .length = 3, .lower = -1 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.{ .integer = 3 }) }, .{});
                try replay.appendWeighted(&.{Datum.typedArray(&value)}, 3);
                try replay.finish();
                const reader = try replay.openReader();
                defer reader.close();
                const row = (try reader.next()).?;
                try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.flatCell(0).value.integer);
                try std.testing.expectEqual(@as(i32, -1), row[0].array.?.dimensions[0].lower);
                try std.testing.expect(row[0].array.?.flatCell(1).sql_null);
            }
            try std.testing.expectEqual(@as(usize, 0), manager.files);
            try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{true});
}

test "SQL replay readers own JSON allocators and fail closed on quota cancellation and corruption" {
    const a = std.testing.allocator;
    const Hook = struct {
        canceled: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.QueryCanceled;
        }
    };
    var hook: Hook = .{};
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &hook, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
    defer manager.deinit();
    for ([_]bool{ false, true }) |to_disk| {
        const replay = try Replay.create(a, 1, if (to_disk) 0 else 64 * 1024, if (to_disk) &manager else null);
        defer replay.deinit();
        {
            const parsed = try std.json.parseFromSlice(std.json.Value, a, "[\"owned\",null]", .{});
            defer parsed.deinit();
            try replay.append(&.{Datum.json(parsed.value)});
        }
        try replay.finish();
        const reader = try replay.openReader();
        defer reader.close();
        const row = (try reader.next()).?;
        const json = row[0].value.array;
        try std.testing.expectEqualStrings("owned", json.items[0].string);
        const arena = if (reader.disk) |run| &run.compact.?.arena else &replay.arena;
        try std.testing.expectEqual(@intFromPtr(arena), @intFromPtr(json.allocator.ptr));
    }
    {
        const replay = try Replay.create(a, 1, 0, null);
        defer replay.deinit();
        try std.testing.expectError(error.SqlProgramLimitExceeded, replay.append(&.{.{}}));
        try std.testing.expectError(error.InvalidSqlBackendResponse, replay.finish());
        try std.testing.expectError(error.InvalidSqlBackendResponse, replay.openReader());
    }
    {
        const replay = try Replay.create(a, 1, 0, &manager);
        defer replay.deinit();
        try replay.append(&.{Datum.json(.{ .integer = 7 })});
        try replay.finish();
        const reader = try replay.openReader();
        defer reader.close();
        hook.canceled = true;
        try std.testing.expectError(error.QueryCanceled, reader.next());
        hook.canceled = false;
        // Corrupt the payload before the first successful block read. Existing
        // framing/checksum validation must be retained by independent readers.
        try replay.disk.?.file.writeRaw(25, &.{255});
        try replay.disk.?.file.flush();
        try std.testing.expectError(error.InvalidSqlSpill, reader.next());
    }
    {
        manager.max_bytes = 1;
        const replay = try Replay.create(a, 1, 0, &manager);
        defer replay.deinit();
        // Flush can fail after a row was admitted into the bounded block.
        replay.append(&.{Datum.json(.{ .string = "quota" })}) catch |err| {
            try std.testing.expectEqual(error.SqlProgramLimitExceeded, err);
            return;
        };
        try std.testing.expectError(error.SqlProgramLimitExceeded, replay.finish());
    }
}

test "SQL weighted replay preserves exact bags across spill and independent expanding readers" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]usize{ 1, 65536 }) |memory_bytes| {
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
        defer manager.deinit();
        const replay = try Replay.create(a, 1, memory_bytes, &manager);
        defer replay.deinit();
        try replay.appendWeighted(&.{Datum.json(.{ .integer = 7 })}, 3);
        try replay.appendWeighted(&.{.{}}, std.math.maxInt(u64));
        var count_arena = std.heap.ArenaAllocator.init(a);
        defer count_arena.deinit();
        const huge = try Count.init(1 << 63).multiply(count_arena.allocator(), Count.init(1 << 40));
        try replay.appendCount(&.{Datum.json(.{ .integer = 9 })}, huge);
        try replay.finish();
        const compact = try replay.openReader();
        defer compact.close();
        const expanded = try replay.openReader();
        defer expanded.close();
        const first = (try compact.nextWeighted()).?;
        try std.testing.expectEqual(@as(u64, 3), first.weight);
        try std.testing.expectEqual(@as(i64, 7), first.values[0].value.integer);
        for (0..3) |_| try std.testing.expectEqual(@as(i64, 7), (try expanded.next()).?[0].value.integer);
        try std.testing.expect((try expanded.next()).?[0].sql_null);
        const second = (try compact.nextWeighted()).?;
        try std.testing.expectEqual(std.math.maxInt(u64), second.weight);
        try std.testing.expect(second.values[0].sql_null);
        const third = (try compact.nextWeighted()).?;
        try std.testing.expectEqual(@as(i64, 9), third.values[0].value.integer);
        try std.testing.expect(third.count.exact() == null);
        try std.testing.expectEqual(huge.scaled().exponent, third.count.scaled().exponent);
        try std.testing.expectEqual(huge.scaled().fraction, third.count.scaled().fraction);
        try std.testing.expect((try compact.nextWeighted()) == null);
        const large = try replay.openReader();
        defer large.close();
        _ = try large.nextWeighted();
        _ = try large.nextWeighted();
        for (0..1024) |_| try std.testing.expectEqual(@as(i64, 9), (try large.next()).?[0].value.integer);
        try std.testing.expect(large.count_arena.queryCapacity() < 4096);
        expanded.rewind();
        try std.testing.expectEqual(@as(i64, 7), (try expanded.next()).?[0].value.integer);
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const batch = try expanded.nextBatch(page.allocator(), 3);
        try std.testing.expectEqual(@as(usize, 3), batch.len());
        try std.testing.expectEqual(@as(i64, 7), (try batch.cell(page.allocator(), 0, 0)).value.integer);
        try std.testing.expectEqual(@as(i64, 7), (try batch.cell(page.allocator(), 1, 0)).value.integer);
        try std.testing.expect((try batch.cell(page.allocator(), 2, 0)).sql_null);
        const more = try expanded.nextBatch(page.allocator(), 2);
        try std.testing.expectEqual(@as(usize, 2), more.len());
        try std.testing.expect((try more.cell(page.allocator(), 1, 0)).sql_null);
        try std.testing.expectEqual(@as(i64, 7), (try batch.cell(page.allocator(), 0, 0)).value.integer);
    }
}
