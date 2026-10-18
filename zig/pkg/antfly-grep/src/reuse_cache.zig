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

//! Bounded process-local typed memoization. All values own their slices; no
//! source generation, credential, query arena or worker is retained. A lookup
//! copies under the lock, so eviction never leaves a borrowed cache pointer.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub fn retainedClone(comptime T: type, a: Allocator, value: T, depth: usize) error{ OutOfMemory, ResultNestingTooDeep }!T {
    if (depth > 512) return error.ResultNestingTooDeep;
    if (T == std.json.Value) return switch (value) {
        .string => |v| .{ .string = try a.dupe(u8, v) },
        .number_string => |v| .{ .number_string = try a.dupe(u8, v) },
        .array => |v| blk: {
            var out = std.json.Array.init(a);
            for (v.items) |item| try out.append(try retainedClone(T, a, item, depth + 1));
            break :blk .{ .array = out };
        },
        .object => |v| blk: {
            var out: std.json.ObjectMap = .empty;
            var it = v.iterator();
            while (it.next()) |entry| try out.put(a, try a.dupe(u8, entry.key_ptr.*), try retainedClone(T, a, entry.value_ptr.*, depth + 1));
            break :blk .{ .object = out };
        },
        else => value,
    };
    return switch (@typeInfo(T)) {
        .optional => |o| if (value) |v| try retainedClone(o.child, a, v, depth + 1) else null,
        .@"struct" => blk: {
            var out: T = undefined;
            inline for (@typeInfo(T).@"struct".field_names) |name| @field(out, name) = try retainedClone(@FieldType(T, name), a, @field(value, name), depth + 1);
            break :blk out;
        },
        .pointer => |ptr| blk: {
            if (ptr.size != .slice) @compileError("retained pointer must be a slice");
            const out = try a.alloc(ptr.child, value.len);
            for (value, out) |v, *dest| dest.* = try retainedClone(ptr.child, a, v, depth + 1);
            break :blk out;
        },
        else => value,
    };
}

pub const Times = struct {
    oldest: i64 = std.math.maxInt(i64),
    newest: i64 = std.math.minInt(i64),
    pub fn add(self: *Times, time: i64) void {
        self.oldest = @min(self.oldest, time);
        self.newest = @max(self.newest, time);
    }
    pub fn merge(self: *Times, other: Times) void {
        self.oldest = @min(self.oldest, other.oldest);
        self.newest = @max(self.newest, other.newest);
    }
    pub fn valid(self: Times, now: i64, ttl: u64) bool {
        return self.oldest != std.math.maxInt(i64) and @as(i128, now) - self.newest >= -300 and @as(i128, now) - self.oldest < ttl;
    }
};
pub fn Cache(comptime Value: type, comptime capacity: usize, comptime limit: usize) type {
    return struct {
        const Self = @This();
        const Entry = struct {
            budget: @import("antfly_regex").Budget,
            arena: std.heap.ArenaAllocator,
            key: [32]u8,
            times: Times,
            value: Value,
        };
        pub const Hit = struct { times: Times, value: Value };
        a: Allocator,
        io: std.Io,
        mutex: std.Io.Mutex = .init,
        slots: [capacity]?*Entry = @splat(null),
        bytes: usize = 0,
        clock: usize = 0,
        pub fn deinit(self: *Self) void {
            for (&self.slots) |*slot| self.remove(slot);
        }
        fn remove(self: *Self, slot: *?*Entry) void {
            if (slot.*) |entry| {
                self.bytes -= entry.budget.live + @sizeOf(Entry);
                entry.arena.deinit();
                self.a.destroy(entry);
                slot.* = null;
            }
        }
        fn index(key: [32]u8) usize {
            return std.mem.readInt(u32, key[0..4], .little) % capacity;
        }
        pub fn get(self: *Self, a: Allocator, key: [32]u8, ttl: u64) !?Hit {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const slot = &self.slots[index(key)];
            const entry = slot.* orelse return null;
            if (!std.mem.eql(u8, &key, &entry.key)) return null;
            if (!entry.times.valid(std.Io.Clock.real.now(self.io).toSeconds(), ttl)) {
                self.remove(slot);
                return null;
            }
            return .{ .times = entry.times, .value = try retainedClone(Value, a, entry.value, 0) };
        }
        pub fn put(self: *Self, key: [32]u8, times: Times, value: Value) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const slot = &self.slots[index(key)];
            self.remove(slot);
            // Include construction in the limit; oversized entries are simply
            // not retained. Reserve at most a quarter for a single value.
            const entry_limit = limit / 4;
            while (limit - self.bytes < entry_limit + @sizeOf(Entry)) {
                self.remove(&self.slots[self.clock % capacity]);
                self.clock +%= 1;
            }
            const entry = self.a.create(Entry) catch return;
            entry.* = .{ .key = key, .times = times, .budget = .{ .backing = self.a, .limit = entry_limit }, .arena = undefined, .value = undefined };
            entry.arena = .init(entry.budget.allocator());
            entry.value = retainedClone(Value, entry.arena.allocator(), value, 0) catch {
                entry.arena.deinit();
                self.a.destroy(entry);
                return;
            };
            self.bytes += entry.budget.live + @sizeOf(Entry);
            slot.* = entry;
        }
    };
}

test "oldest rejected evidence controls expiry and future timestamps fail closed" {
    var times: Times = .{};
    times.add(100);
    times.add(150);
    try std.testing.expect(times.valid(199, 100));
    try std.testing.expect(!times.valid(200, 100));
    try std.testing.expect(!times.valid(-151, 100));
    try std.testing.expect(!(Times{}).valid(100, 100));
}

test "typed cache owns slices, evicts oversized values and expires without sliding" {
    const C = Cache([]const u8, 4, 16384);
    var cache: C = .{ .a = std.testing.allocator, .io = std.testing.io };
    defer cache.deinit();
    var times: Times = .{};
    times.add(std.Io.Clock.real.now(std.testing.io).toSeconds());
    var value = [_]u8{ 'a', 'b' };
    cache.put(@splat(0), times, &value);
    value[0] = 'z';
    const hit = (try cache.get(std.testing.allocator, @splat(0), 100)).?;
    defer std.testing.allocator.free(hit.value);
    try std.testing.expectEqualStrings("ab", hit.value);
    try std.testing.expect((try cache.get(std.testing.allocator, @splat(0), 0)) == null);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    var oversized: [8192]u8 = @splat('x');
    cache.put(@splat(0), times, &oversized);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
}
