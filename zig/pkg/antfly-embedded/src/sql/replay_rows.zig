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

pub const Replay = struct {
    a: A,
    width: usize,
    memory: Budget,
    arena: std.heap.ArenaAllocator,
    rows: std.ArrayList([]const Datum) = .empty,
    manager: ?*spill.Manager,
    disk: ?spill.Sequential = null,
    count: usize = 0,
    readers: usize = 0,
    finished: bool = false,
    failed: bool = false,

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
        self.arena.deinit();
        std.debug.assert(self.memory.live == 0);
        const a = self.a;
        a.destroy(self);
    }

    fn appendMemory(self: *Replay, values: []const Datum) !void {
        try self.rows.ensureUnusedCapacity(self.memory.allocator(), 1);
        const a = self.arena.allocator();
        const row = try a.alloc(Datum, values.len);
        for (values, row) |value, *out| out.* = try operators.cloneDatum(a, value);
        self.rows.appendAssumeCapacity(row);
    }

    fn startSpill(self: *Replay) !void {
        const manager = self.manager orelse return error.SqlProgramLimitExceeded;
        self.disk = try spill.Sequential.init(manager, @max(128, manager.buffer_bytes *| 2));
        for (self.rows.items, 0..) |row, ordinal| _ = try self.disk.?.append(.{ .values = row, .keys = &.{}, .ordinal = ordinal }, spill.none);
        self.rows.clearAndFree(self.memory.allocator());
        _ = self.arena.reset(.free_all);
        std.debug.assert(self.memory.live == 0);
    }

    pub fn append(self: *Replay, values: []const Datum) !void {
        if (self.failed or self.finished or values.len != self.width) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (self.count == std.math.maxInt(usize)) return error.SqlProgramLimitExceeded;
        if (self.disk == null) self.appendMemory(values) catch |err| {
            if (err != error.OutOfMemory or !self.memory.exhausted) return err;
            try self.startSpill();
        };
        if (self.disk) |*run| _ = try run.append(.{ .values = values, .keys = &.{}, .ordinal = self.count }, spill.none);
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
        reader.* = .{ .owner = self, .disk = if (self.disk) |*run| try run.openReader() else null };
        self.readers += 1;
        return reader;
    }

    pub const Reader = struct {
        owner: *Replay,
        disk: ?spill.Sequential.ReplayReader,
        index: usize = 0,

        /// A borrowed row survives other readers, but not this reader's next
        /// block load or close. Retaining operators copy at their boundary.
        pub fn next(self: *Reader) !?[]const Datum {
            if (self.index == self.owner.count) return null;
            const values = if (self.disk) |*run| (try run.readBorrowed(self.index)).row.values else self.owner.rows.items[self.index];
            self.index += 1;
            return values;
        }

        pub fn rewind(self: *Reader) void {
            self.index = 0;
        }

        pub fn close(self: *Reader) void {
            if (self.disk) |*run| run.deinit();
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
                try replay.append(&.{Datum.typedArray(&value)});
                try replay.finish();
                const reader = try replay.openReader();
                defer reader.close();
                const row = (try reader.next()).?;
                try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.elements[0].value.integer);
                try std.testing.expectEqual(@as(i32, -1), row[0].array.?.dimensions[0].lower);
                try std.testing.expect(row[0].array.?.elements[1].sql_null);
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
        const arena = if (reader.disk) |*run| &run.read_arena else &replay.arena;
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
