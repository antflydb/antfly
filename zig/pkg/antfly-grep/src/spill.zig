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

//! Bounded external relevance sort. Payloads are appended once; sorted key runs
//! merge in binary levels. Memory is bounded independently of result count/bytes.
const std = @import("std");
const Temp = @import("temporary.zig").File;
const A = std.mem.Allocator;
pub const Entry = struct { path: []const u8, score: f64, ordinal: usize, offset: u64, length: u64 };
fn less(_: void, x: Entry, y: Entry) bool {
    if (x.score != y.score) return x.score > y.score;
    const order = std.mem.order(u8, x.path, y.path);
    return if (order == .eq) x.ordinal < y.ordinal else order == .lt;
}
fn encode(writer: *std.Io.Writer, entry: Entry) !void {
    var header: [36]u8 = undefined;
    std.mem.writeInt(u64, header[0..8], @bitCast(entry.score), .little);
    std.mem.writeInt(u64, header[8..16], entry.ordinal, .little);
    std.mem.writeInt(u64, header[16..24], entry.offset, .little);
    std.mem.writeInt(u64, header[24..32], entry.length, .little);
    std.mem.writeInt(u32, header[32..36], @intCast(entry.path.len), .little);
    try writer.writeAll(&header);
    try writer.writeAll(entry.path);
}
fn decode(a: A, reader: *std.Io.Reader) !?Entry {
    var h: [36]u8 = undefined;
    const n = try reader.readSliceShort(&h);
    if (n == 0) return null;
    if (n != h.len) return error.InvalidSpill;
    const size = std.mem.readInt(u32, h[32..36], .little);
    if (size > 1024 * 1024) return error.InvalidSpill;
    const path = try a.alloc(u8, size);
    errdefer a.free(path);
    try reader.readSliceAll(path);
    return .{ .path = path, .score = @bitCast(std.mem.readInt(u64, h[0..8], .little)), .ordinal = @intCast(std.mem.readInt(u64, h[8..16], .little)), .offset = std.mem.readInt(u64, h[16..24], .little), .length = std.mem.readInt(u64, h[24..32], .little) };
}
pub const Store = struct {
    limits: *@import("budget.zig").Limits,
    max_bytes: usize,
    reserved: usize = 0,
    a: A,
    io: std.Io,
    directory: ?[]const u8,
    payload: ?Temp = null,
    memory: std.ArrayList(u8) = .empty,
    payload_bytes: u64 = 0,
    items: std.ArrayList(Entry) = .empty,
    key_bytes: usize = 0,
    levels: [64]?Temp = @splat(null),
    mutex: std.Io.Mutex = .init,
    count: usize = 0,
    pub const memory_limit = 512 * 1024;
    pub const payload_memory_limit = 256 * 1024;
    pub fn deinit(self: *Store) void {
        _ = self.limits.spill_bytes.fetchSub(self.reserved, .monotonic);
        for (self.items.items) |entry| self.a.free(entry.path);
        self.items.deinit(self.a);
        self.memory.deinit(self.a);
        for (&self.levels) |*run| if (run.*) |*file| file.deinit(self.a, self.io);
        if (self.payload) |*file| file.deinit(self.a, self.io);
    }
    pub fn add(self: *Store, path: []const u8, score: f64, ordinal: usize, body: []const u8) !void {
        if (body.len == 0) return;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const key_size = try std.math.add(usize, path.len, 36);
        const reservation = try std.math.add(usize, body.len, try std.math.mul(usize, key_size, 2));
        if (!@import("budget.zig").Limits.reserve(&self.limits.spill_bytes, reservation, self.max_bytes)) return error.SpillBudgetExceeded;
        self.reserved += reservation;
        if (self.key_bytes + path.len + @sizeOf(Entry) > memory_limit) try self.flush();
        const owned = try self.a.dupe(u8, path);
        errdefer self.a.free(owned);
        if (self.payload == null and self.payload_bytes + body.len <= payload_memory_limit) {
            try self.memory.appendSlice(self.a, body);
        } else {
            if (self.payload == null) {
                self.payload = try Temp.create(self.a, self.io, self.directory);
                try self.payload.?.file.writePositionalAll(self.io, self.memory.items, 0);
                self.memory.deinit(self.a);
                self.memory = .empty;
            }
            try self.payload.?.file.writePositionalAll(self.io, body, self.payload_bytes);
        }
        try self.items.append(self.a, .{ .path = owned, .score = score, .ordinal = ordinal, .offset = self.payload_bytes, .length = body.len });
        self.payload_bytes += body.len;
        self.key_bytes += path.len + @sizeOf(Entry);
        self.count += 1;
    }
    fn merge(self: *Store, left: Temp, right: Temp) !Temp {
        var output = try Temp.create(self.a, self.io, self.directory);
        errdefer output.deinit(self.a, self.io);
        var lb: [8192]u8 = undefined;
        var rb: [8192]u8 = undefined;
        var wb: [8192]u8 = undefined;
        var lr = left.file.reader(self.io, &lb);
        var rr = right.file.reader(self.io, &rb);
        var writer = output.file.writer(self.io, &wb);
        var l = try decode(self.a, &lr.interface);
        defer if (l) |entry| self.a.free(entry.path);
        var r = try decode(self.a, &rr.interface);
        defer if (r) |entry| self.a.free(entry.path);
        while (l != null or r != null) {
            const take_left = r == null or (l != null and less({}, l.?, r.?));
            const selected = if (take_left) &l else &r;
            const entry = selected.*.?;
            try encode(&writer.interface, entry);
            self.a.free(entry.path);
            selected.* = null;
            selected.* = try decode(self.a, if (take_left) &lr.interface else &rr.interface);
        }
        try writer.interface.flush();
        return output;
    }
    fn publish(self: *Store, input: Temp) !void {
        var carry = input;
        errdefer carry.deinit(self.a, self.io);
        for (&self.levels) |*level| {
            if (level.*) |*old| {
                const combined = try self.merge(old.*, carry);
                old.deinit(self.a, self.io);
                level.* = null;
                carry.deinit(self.a, self.io);
                carry = combined;
            } else {
                level.* = carry;
                return;
            }
        }
        return error.SpillTooManyRuns;
    }
    fn flush(self: *Store) !void {
        if (self.items.items.len == 0) return;
        std.mem.sort(Entry, self.items.items, {}, less);
        var run = try Temp.create(self.a, self.io, self.directory);
        var buffer: [8192]u8 = undefined;
        var writer = run.file.writer(self.io, &buffer);
        // publish consumes the run even on failure.
        self.writeRun(&writer.interface) catch |err| {
            run.deinit(self.a, self.io);
            return err;
        };
        try self.publish(run);
        for (self.items.items) |entry| self.a.free(entry.path);
        self.items.clearRetainingCapacity();
        self.key_bytes = 0;
    }
    fn writeRun(self: *Store, writer: *std.Io.Writer) !void {
        for (self.items.items) |entry| try encode(writer, entry);
        try writer.flush();
    }
    pub fn finish(self: *Store) !void {
        if (!self.hasRuns()) {
            std.mem.sort(Entry, self.items.items, {}, less);
            return;
        }
        try self.flush();
        var selected: ?usize = null;
        for (&self.levels, 0..) |*level, i| if (level.* != null) {
            if (selected) |j| {
                const merged = try self.merge(self.levels[j].?, level.*.?);
                self.levels[j].?.deinit(self.a, self.io);
                level.*.?.deinit(self.a, self.io);
                self.levels[j] = null;
                level.* = merged;
            }
            selected = i;
        };
    }
    fn hasRuns(self: *const Store) bool {
        for (self.levels) |run| if (run != null) return true;
        return false;
    }
    pub fn visit(self: *Store, context: anytype, comptime callback: anytype) !void {
        if (!self.hasRuns()) {
            for (self.items.items) |entry| try callback(context, entry);
            return;
        }
        for (self.levels) |run| if (run) |file| {
            var buffer: [8192]u8 = undefined;
            var reader = file.file.reader(self.io, &buffer);
            while (try decode(self.a, &reader.interface)) |entry| {
                defer self.a.free(entry.path);
                try callback(context, entry);
            }
        };
    }
    pub fn stream(self: *Store, entry: Entry, context: anytype, comptime callback: anytype) !void {
        if (self.payload == null) {
            if (entry.offset > self.memory.items.len or entry.length > self.memory.items.len - entry.offset) return error.InvalidSpill;
            const start: usize = @intCast(entry.offset);
            try callback(context, self.memory.items[start..][0..@intCast(entry.length)]);
            return;
        }
        var offset = entry.offset;
        var remaining = entry.length;
        var buffer: [65536]u8 = undefined;
        while (remaining > 0) {
            const n = try self.payload.?.file.readPositionalAll(self.io, buffer[0..@min(buffer.len, remaining)], offset);
            if (n == 0) return error.InvalidSpill;
            try callback(context, buffer[0..n]);
            offset += n;
            remaining -= n;
        }
    }
};

test "external runs merge stable ties and replay payloads with bounded keys" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var limits: @import("budget.zig").Limits = .{};
    {
        var store: Store = .{ .a = a, .io = io, .directory = directory, .limits = &limits, .max_bytes = 16 * 1024 * 1024 };
        defer store.deinit();
        var name: [64]u8 = undefined;
        for (0..30000) |i| {
            const path = try std.fmt.bufPrint(&name, "file-{d:0>6}", .{29999 - i});
            try store.add(path, @as(f64, @floatFromInt(i % 5)) / 5, i, path);
            try std.testing.expect(store.key_bytes <= Store.memory_limit);
        }
        try store.finish();
        try std.testing.expect(store.payload != null);
        try std.testing.expectEqual(@as(usize, 0), store.memory.items.len);
        const Check = struct {
            count: usize = 0,
            previous_score: f64 = 1,
            previous_path: [64]u8 = undefined,
            previous_len: usize = 0,
            store: *Store,
            fn row(c: *@This(), entry: Entry) !void {
                try std.testing.expect(entry.score <= c.previous_score);
                if (entry.score == c.previous_score) try std.testing.expect(std.mem.order(u8, c.previous_path[0..c.previous_len], entry.path) == .lt);
                @memcpy(c.previous_path[0..entry.path.len], entry.path);
                c.previous_len = entry.path.len;
                c.previous_score = entry.score;
                var bytes: std.Io.Writer.Allocating = .init(std.testing.allocator);
                defer bytes.deinit();
                try c.store.stream(entry, &bytes.writer, std.Io.Writer.writeAll);
                try std.testing.expectEqualStrings(entry.path, bytes.written());
                c.count += 1;
            }
        };
        for (0..2) |_| {
            var check: Check = .{ .store = &store };
            try store.visit(&check, Check.row);
            try std.testing.expectEqual(@as(usize, 30000), check.count);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), limits.spill_bytes.load(.monotonic));
    var dir = try std.Io.Dir.cwd().openDir(io, directory, .{ .iterate = true });
    defer dir.close(io);
    var it = dir.iterate();
    try std.testing.expect(try it.next(io) == null);
}

test "spill budget admits complete payloads and releases reservations" {
    var limits: @import("budget.zig").Limits = .{};
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    {
        var store: Store = .{ .a = std.testing.allocator, .io = io, .directory = path, .limits = &limits, .max_bytes = 100 };
        defer store.deinit();
        try store.add("a", 1, 0, "ok");
        try std.testing.expectError(error.SpillBudgetExceeded, store.add("b", 1, 1, "too much"));
        try std.testing.expectEqual(@as(usize, 1), store.count);
        try store.finish();
        try std.testing.expect(store.payload == null);
        const Check = struct {
            fn row(s: *Store, entry: Entry) !void {
                var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
                defer writer.deinit();
                try s.stream(entry, &writer.writer, std.Io.Writer.writeAll);
                try std.testing.expectEqualStrings("ok", writer.written());
            }
        };
        try store.visit(&store, Check.row);
    }
    try std.testing.expectEqual(@as(usize, 0), limits.spill_bytes.load(.monotonic));
}
