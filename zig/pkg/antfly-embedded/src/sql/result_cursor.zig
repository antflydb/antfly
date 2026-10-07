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

//! Statement-owned blocking results shared by nested relations and public streams.
const std = @import("std");

pub const Cursor = struct {
    manager: @import("spill.zig").Manager,
    shared: ?*@import("spill.zig").Manager = null,
    a: std.mem.Allocator,
    width: usize,
    rows: @import("spill.zig").Sequential,
    memory: ?*@import("replay_rows.zig").Replay = null,
    memory_reader: ?*@import("replay_rows.zig").Replay.Reader = null,
    sealed: bool = false,
    replay_failed: bool = false,
    replay_readers: usize = 0,
    borrowed_sorted: ?usize = null,
    index: usize = 0,
    sorted: ?*@import("operators.zig").TopK = null,
    sorted_rows: []const @import("operators.zig").Row = &.{},
    sorted_offset: usize = 0,
    sorted_count: usize = 0,
    /// Borrow the statement spill owner, sharing its quota and cancellation.
    pub fn create(a: std.mem.Allocator, manager: *@import("spill.zig").Manager, width: usize) !*Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        self.* = .{ .manager = undefined, .shared = manager, .a = a, .width = width, .rows = try @import("spill.zig").Sequential.init(manager, @max(128, manager.buffer_bytes * 2)) };
        return self;
    }

    /// No-I/O execution uses the same lossless cursor and sort ownership
    /// transfer. Replay enforces a resident quota without a filesystem fallback.
    pub fn createMemory(a: std.mem.Allocator, width: usize, memory_bytes: usize) !*Cursor {
        const self = try a.create(Cursor);
        errdefer a.destroy(self);
        const memory = try @import("replay_rows.zig").Replay.create(a, width, memory_bytes, null);
        self.* = .{ .manager = undefined, .a = a, .width = width, .rows = undefined, .memory = memory };
        return self;
    }
    pub fn next(self: *Cursor, a: std.mem.Allocator) !?[]const @import("scalar.zig").Datum {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        self.releaseBorrowed();
        if (self.index == self.count()) return null;
        const row = try self.read(a);
        if (self.ownsRead()) {
            self.index += 1;
            return row.values;
        }
        const values = try a.alloc(@import("scalar.zig").Datum, row.values.len);
        for (row.values, values) |value, *out| out.* = try @import("operators.zig").cloneDatum(a, value);
        if (!self.sealed) if (self.sorted) |top| if (top.external == null) top.releaseFinishedRow(self.sorted_offset + self.index);
        self.index += 1;
        return values;
    }

    /// One-pass ownership contract: resident rows are borrowed, external-sort
    /// values belong to a, and only a retaining consumer copies a payload. A
    /// resident sorted row expires on the next advancement (including EOF),
    /// reclaiming its arena before the next mutation image is prepared.
    pub fn nextBorrowed(self: *Cursor, a: std.mem.Allocator) !?[]const @import("scalar.zig").Datum {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        self.releaseBorrowed();
        if (self.index == self.count()) return null;
        const row = try self.read(a);
        if (!self.sealed) if (self.sorted) |top| if (top.external == null) {
            self.borrowed_sorted = self.sorted_offset + self.index;
        };
        self.index += 1;
        return row.values;
    }

    fn releaseBorrowed(self: *Cursor) void {
        if (self.borrowed_sorted) |index| {
            self.sorted.?.releaseFinishedRow(index);
            self.borrowed_sorted = null;
        }
    }
    pub fn count(self: *const Cursor) usize {
        return if (self.sorted != null) self.sorted_count else if (self.memory) |memory| memory.count else @intCast(self.rows.size);
    }
    pub fn takeSorted(raw: *anyopaque, top: *@import("operators.zig").TopK, offset: usize, limit: usize, implicit: bool) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.replay_failed or self.sealed or self.sorted != null or self.count() != 0) return error.InvalidSqlBackendResponse;
        const total = if (top.external) |sort| @min(sort.total, top.capacity) else top.count;
        const available = total -| offset;
        if (implicit and available > limit) return error.SqlResultTooLarge;
        const owned = try self.a.create(@import("operators.zig").TopK);
        errdefer self.a.destroy(owned);
        // Finish before moving: memory rows borrow the heap's stable arenas.
        const memory_rows = if (top.external == null) try top.finish(self.a) else &.{};
        owned.* = top.*;
        top.* = .{ .alloc = top.alloc, .entries = &.{}, .orders = &.{}, .max_bytes = 0, .retained_bytes = 0 };
        self.sorted = owned;
        self.sorted_rows = memory_rows;
        self.sorted_offset = @min(offset, total);
        self.sorted_count = @min(available, limit);
        if (owned.external == null) for (0..self.sorted_offset) |index| owned.releaseFinishedRow(index);
    }
    pub fn ownsRead(self: *const Cursor) bool {
        return if (self.sorted) |top| top.external != null else false;
    }
    pub fn read(self: *Cursor, a: std.mem.Allocator) !@import("operators.zig").Row {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        if (self.sorted) |top| {
            if (top.external) |sort| {
                var scratch = std.heap.ArenaAllocator.init(self.a);
                defer scratch.deinit();
                while (self.sorted_offset != 0) : (self.sorted_offset -= 1) {
                    _ = scratch.reset(.free_all);
                    _ = (try sort.nextValues(scratch.allocator())) orelse return error.InvalidSqlSpill;
                }
                return (try sort.nextValues(a)) orelse error.InvalidSqlSpill;
            }
            return self.sorted_rows[self.sorted_offset + self.index];
        }
        if (self.memory) |memory| {
            if (self.memory_reader == null) {
                try memory.finish();
                self.memory_reader = try memory.openReader();
            }
            return .{ .values = (try self.memory_reader.?.next()) orelse return error.InvalidSqlBackendResponse, .keys = &.{}, .ordinal = self.index };
        }
        return (try self.rows.readBorrowed(self.index)).row;
    }
    pub fn append(raw: *anyopaque, values: []const @import("scalar.zig").Datum) !void {
        const self: *Cursor = @ptrCast(@alignCast(raw));
        if (self.replay_failed or self.sealed or values.len != self.width or self.sorted != null) return error.InvalidSqlBackendResponse;
        if (self.memory) |memory| return memory.append(values);
        _ = try self.rows.append(.{ .values = values, .keys = &.{}, .ordinal = self.rows.size }, @import("spill.zig").none);
    }
    pub fn close(self: *Cursor) void {
        std.debug.assert(self.replay_readers == 0);
        const a = self.a;
        if (self.sorted) |top| {
            a.free(self.sorted_rows);
            top.deinit();
            a.destroy(top);
        }
        if (self.memory) |memory| {
            if (self.memory_reader) |reader| reader.close();
            memory.deinit();
        } else {
            self.rows.close();
            if (self.shared == null) self.manager.deinit();
        }
        a.destroy(self);
    }

    /// Freeze a capture and start an independent borrowed pass. Resident sorted
    /// rows and existing runs are reused without copying. A forward-only sort
    /// merge must be sealed once into bounded final blocks before replay; it
    /// cannot be rewound. Partially consumed sorts reject admission because
    /// their earlier rows may already have been retired.
    pub fn openReplayReader(self: *Cursor) !*ReplayReader {
        if (self.replay_failed) return error.InvalidSqlBackendResponse;
        if (!self.sealed) {
            if (self.sorted) |top| {
                if (self.index != 0) return error.InvalidSqlBackendResponse;
                if (top.external != null) {
                    // A sort has no resident final run. Drain only its selected
                    // offset/limit window, preserving complete Datum values.
                    // Failed admission cannot retry a partially advanced merge.
                    errdefer self.replay_failed = true;
                    if (self.memory != null or self.rows.size != 0) return error.InvalidSqlBackendResponse;
                    var scratch = std.heap.ArenaAllocator.init(self.a);
                    defer scratch.deinit();
                    for (0..self.sorted_count) |ordinal| {
                        _ = scratch.reset(.retain_capacity);
                        const row = try self.read(scratch.allocator());
                        _ = try self.rows.append(.{ .values = row.values, .keys = &.{}, .ordinal = ordinal }, @import("spill.zig").none);
                    }
                    try self.rows.seal();
                    self.a.free(self.sorted_rows);
                    top.deinit();
                    self.a.destroy(top);
                    self.sorted = null;
                    self.sorted_rows = &.{};
                }
            }
            if (self.memory) |memory| try memory.finish() else try self.rows.seal();
            self.sealed = true;
        }
        const reader = try self.a.create(ReplayReader);
        errdefer self.a.destroy(reader);
        reader.* = .{
            .owner = self,
            .storage = if (self.sorted != null) .sorted else if (self.memory) |memory| .{ .memory = try memory.openReader() } else .{ .disk = try self.rows.openReader() },
        };
        self.replay_readers += 1;
        return reader;
    }

    /// Stable-address reader owned by the cursor allocator. Borrowed values
    /// survive reads by other readers, but not this reader's next block load
    /// or close. Consumers retaining a value must copy at their owned boundary.
    pub const ReplayReader = struct {
        owner: *Cursor,
        storage: union(enum) {
            sorted,
            memory: *@import("replay_rows.zig").Replay.Reader,
            disk: @import("spill.zig").Sequential.Reader,
        },
        index: usize = 0,

        pub fn next(self: *ReplayReader) !?[]const @import("scalar.zig").Datum {
            if (self.index == self.owner.count()) return null;
            const values = switch (self.storage) {
                .sorted => self.owner.sorted_rows[self.owner.sorted_offset + self.index].values,
                .memory => |reader| (try reader.next()) orelse return error.InvalidSqlBackendResponse,
                .disk => |*reader| (try reader.readBorrowed(self.index)).row.values,
            };
            if (values.len != self.owner.width) return error.InvalidSqlBackendResponse;
            self.index += 1;
            return values;
        }

        pub fn rewind(self: *ReplayReader) void {
            self.index = 0;
            if (self.storage == .memory) self.storage.memory.rewind();
        }

        pub fn close(self: *ReplayReader) void {
            const owner = self.owner;
            switch (self.storage) {
                .sorted => {},
                .memory => |reader| reader.close(),
                .disk => |*reader| reader.deinit(),
            }
            owner.replay_readers -= 1;
            owner.a.destroy(self);
        }
    };
};

test "SQL no-I/O blocking cursors preserve arrays bound memory and unwind allocation failures" {
    const Scenario = struct {
        fn run(a: std.mem.Allocator) !void {
            const Datum = @import("scalar.zig").Datum;
            var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
            const cursor = try Cursor.createMemory(a, 1, 64 * 1024);
            defer cursor.close();
            try Cursor.append(cursor, &.{Datum.typedArray(&array)});
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const row = (try cursor.next(arena.allocator())).?;
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.elements[0].value.integer);
            try std.testing.expectEqual(@as(i32, -2), row[0].array.?.dimensions[0].lower);
            try std.testing.expect(row[0].array.?.elements[1].sql_null);
            try std.testing.expect(try cursor.next(arena.allocator()) == null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
    const cursor = try Cursor.createMemory(std.testing.allocator, 1, 0);
    defer cursor.close();
    try std.testing.expectError(error.SqlProgramLimitExceeded, Cursor.append(cursor, &.{.{}}));
}

test "SQL sealed typed captures replay independent borrowed passes without a second spool" {
    const Scenario = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn run(a: std.mem.Allocator, disk: bool) !void {
            const Datum = @import("scalar.zig").Datum;
            const arrays = @import("array_value.zig");
            var token: u8 = 0;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &token, .checkpoint = checkpoint, .compression = .none };
            defer manager.deinit();
            {
                const cursor = if (disk) try Cursor.create(a, &manager, 4) else try Cursor.createMemory(a, 4, 2 * 1024 * 1024);
                defer cursor.close();
                var elements = [_]Datum{ Datum.json(.{ .integer = 9007199254740993 }), .{} };
                var array = try arrays.Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &elements, .{});
                var json = try arrays.Value.init(.jsonb, &.{.{ .length = 2 }}, &.{ Datum.json(.null), .{} }, .{});
                // Allocation-fault coverage needs only a small resident run;
                // the disk fixture crosses several encoded block boundaries.
                const count: usize = if (disk) 1024 else 2;
                for (0..count) |_| try Cursor.append(cursor, &.{ Datum.typedArray(&array), .{}, Datum.json(.null), Datum.typedArray(&json) });
                elements[0].value = .{ .integer = 42 };
                const first = try cursor.openReplayReader();
                defer first.close();
                const second = try cursor.openReplayReader();
                defer second.close();
                try std.testing.expectError(error.InvalidSqlBackendResponse, Cursor.append(cursor, &.{ .{}, .{}, .{}, .{} }));
                const retained = (try first.next()).?;
                const files = manager.files;
                for (0..count) |_| {
                    const row = (try second.next()).?;
                    try verify(row);
                    if (!disk) try std.testing.expect(retained[0].array == row[0].array or second.index != 1);
                }
                try std.testing.expect(try second.next() == null);
                try verify(retained); // The other reader cannot retire our block.
                for (0..3) |_| {
                    first.rewind();
                    for (0..count) |_| try verify((try first.next()).?);
                    try std.testing.expect(try first.next() == null);
                }
                second.rewind();
                try verify((try second.next()).?);
                try std.testing.expectEqual(files, manager.files);
                try std.testing.expectEqual(@as(usize, if (disk) 1 else 0), files);
                try std.testing.expectEqual(@as(usize, 0), cursor.index);
                // Existing owning consumption remains independent of replay.
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                try verify((try cursor.next(scratch.allocator())).?);
            }
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
        fn verify(row: []const @import("scalar.zig").Datum) !void {
            try std.testing.expectEqual(@as(usize, 4), row.len);
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.elements[0].value.integer);
            try std.testing.expectEqual(@as(i32, -2), row[0].array.?.dimensions[0].lower);
            try std.testing.expect(row[0].array.?.elements[1].sql_null);
            try std.testing.expect(row[1].sql_null);
            try std.testing.expect(!row[2].sql_null and row[2].value == .null);
            try std.testing.expect(!row[3].array.?.elements[0].sql_null);
            try std.testing.expect(row[3].array.?.elements[0].value == .null);
            try std.testing.expect(row[3].array.?.elements[1].sql_null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{false});
    try Scenario.run(std.testing.allocator, true);
}

test "SQL replay freezes resident sorted owners and seals external sort windows once" {
    const Scenario = struct {
        fn checkpoint(ptr: *anyopaque) !void {
            if (@as(*bool, @ptrCast(@alignCast(ptr))).*) return error.QueryCanceled;
        }
        fn run(a: std.mem.Allocator, disk: bool, cancel: bool, consumed: bool) !void {
            const operators = @import("operators.zig");
            const Datum = @import("scalar.zig").Datum;
            var canceled = false;
            var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &canceled, .checkpoint = checkpoint, .compression = .none };
            defer manager.deinit();
            {
                const cursor = if (disk) try Cursor.create(a, &manager, 2) else try Cursor.createMemory(a, 2, 64 * 1024);
                defer cursor.close();
                const count: usize = if (disk) 256 else 8;
                var top = try operators.TopK.initWithSpill(a, count, &.{.{ .descending = true }}, if (disk) 16 * 1024 else 64 * 1024, if (disk) &manager else null);
                defer top.deinit();
                var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
                for (0..count) |ordinal| {
                    const id = Datum.json(.{ .integer = @intCast(ordinal) });
                    try top.add(.{ .values = &.{ id, Datum.typedArray(&array) }, .keys = &.{id}, .ordinal = ordinal });
                }
                try Cursor.takeSorted(cursor, &top, 2, 3, false);
                try std.testing.expectEqual(disk, cursor.sorted.?.external != null);
                var scratch = std.heap.ArenaAllocator.init(a);
                defer scratch.deinit();
                if (consumed) {
                    _ = (try cursor.next(scratch.allocator())).?;
                    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.openReplayReader());
                    return;
                }
                canceled = cancel;
                if (cancel) {
                    try std.testing.expectError(error.QueryCanceled, cursor.openReplayReader());
                    canceled = false;
                    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.openReplayReader());
                    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.next(scratch.allocator()));
                    return;
                }
                const first = try cursor.openReplayReader();
                defer first.close();
                const second = try cursor.openReplayReader();
                defer second.close();
                try std.testing.expectEqual(@as(usize, 3), cursor.count());
                if (disk) {
                    try std.testing.expect(cursor.sorted == null);
                    try std.testing.expectEqual(@as(usize, 1), manager.files);
                } else try std.testing.expect(cursor.sorted != null);
                const row = (try first.next()).?;
                if (!disk) try std.testing.expect(row[1].array == cursor.sorted_rows[cursor.sorted_offset].values[1].array);
                for (0..3) |_| {
                    second.rewind();
                    for (0..3) |ordinal| {
                        const value = (try second.next()).?;
                        try std.testing.expectEqual(@as(i64, @intCast(count - 3 - ordinal)), value[0].value.integer);
                        try std.testing.expectEqual(@as(i64, 9007199254740993), value[1].array.?.elements[0].value.integer);
                        try std.testing.expectEqual(@as(i32, -2), value[1].array.?.dimensions[0].lower);
                        try std.testing.expect(value[1].array.?.elements[1].sql_null);
                    }
                    try std.testing.expect(try second.next() == null);
                }
                _ = (try cursor.next(scratch.allocator())).?;
                try std.testing.expectEqual(@as(i64, 9007199254740993), row[1].array.?.elements[0].value.integer);
            }
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{ false, false, false });
    try Scenario.run(std.testing.allocator, false, false, true);
    try Scenario.run(std.testing.allocator, true, false, false);
    try Scenario.run(std.testing.allocator, true, false, true);
    try Scenario.run(std.testing.allocator, true, true, false);
}

test "SQL forward borrowed sorted rows allocate nothing and retire before the next image" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const cursor = try Cursor.createMemory(a, 1, 64 * 1024);
    defer cursor.close();
    var top = try @import("operators.zig").TopK.init(a, 4, &.{.{}}, 64 * 1024);
    defer top.deinit();
    var array = try @import("array_value.zig").Value.init(.int64, &.{.{ .length = 2, .lower = -2 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
    for (0..4) |ordinal| try top.add(.{ .values = &.{Datum.typedArray(&array)}, .keys = &.{Datum.json(.{ .integer = @intCast(ordinal) })}, .ordinal = ordinal });
    try Cursor.takeSorted(cursor, &top, 1, 3, false);
    try std.testing.expectEqual(@as(usize, 1), cursor.sorted.?.released);
    for (0..3) |ordinal| {
        // Any accidental per-row cloning is an immediate allocation failure.
        const row = (try cursor.nextBorrowed(std.testing.failing_allocator)).?;
        try std.testing.expectEqual(@as(usize, 1) + ordinal, cursor.sorted.?.released);
        try std.testing.expect(row[0].array == cursor.sorted_rows[1 + ordinal].values[0].array);
        try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].array.?.elements[0].value.integer);
        try std.testing.expectEqual(@as(i32, -2), row[0].array.?.dimensions[0].lower);
        try std.testing.expect(row[0].array.?.elements[1].sql_null);
    }
    try std.testing.expect(try cursor.nextBorrowed(std.testing.failing_allocator) == null);
    try std.testing.expectEqual(@as(usize, 4), cursor.sorted.?.released);
    try std.testing.expectError(error.InvalidSqlBackendResponse, cursor.openReplayReader());
}

test "SQL blocking result blocks preserve exact values and avoid row directories" {
    const a = std.testing.allocator;
    const Datum = @import("scalar.zig").Datum;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none };
    defer manager.deinit();
    {
        const cursor = try Cursor.create(a, &manager, 3);
        defer cursor.close();
        for (0..1024) |_| try Cursor.append(cursor, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.null) });
        try std.testing.expectEqual(@as(usize, 1), manager.files);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var count: usize = 0;
        while (true) {
            _ = arena.reset(.retain_capacity);
            const row = (try cursor.next(arena.allocator())) orelse break;
            try std.testing.expectEqual(@as(i64, 9007199254740993), row[0].value.integer);
            try std.testing.expect(row[1].sql_null);
            try std.testing.expect(!row[2].sql_null and row[2].value == .null);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1024), count);
        try std.testing.expect(manager.read_calls < 128);
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}
