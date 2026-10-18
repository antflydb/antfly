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
//! leases pin immutable values; eviction never invalidates an active reader.
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
        const ways = @min(4, capacity);
        const reservation = limit / 4 + @sizeOf(Entry);
        const Entry = struct {
            owner: *Self,
            refs: std.atomic.Value(usize) = .init(1),
            budget: @import("antfly_regex").Budget,
            arena: std.heap.ArenaAllocator,
            key: [32]u8,
            token: [32]u8,
            stamp: u64 = 0,
            times: Times,
            value: Value,
            fn release(entry: *Entry) void {
                if (entry.refs.fetchSub(1, .acq_rel) != 1) return;
                const owner = entry.owner;
                _ = owner.physical.fetchSub(entry.budget.live + @sizeOf(Entry), .monotonic);
                entry.arena.deinit();
                owner.a.destroy(entry);
            }
        };
        pub const Hit = struct { times: Times, value: Value };
        pub const Lease = struct {
            entry: *Entry,
            times: Times,
            value: Value,
            token: [32]u8,
            pub fn release(self: Lease) void {
                self.entry.release();
            }
        };
        a: Allocator,
        io: std.Io,
        mutex: std.Io.Mutex = .init,
        slots: [capacity]?*Entry = @splat(null),
        bytes: usize = 0,
        physical: std.atomic.Value(usize) = .init(0),
        clock: usize = 0,
        stamp: u64 = 0,
        pub fn deinit(self: *Self) void {
            for (&self.slots) |*slot| self.remove(slot);
        }
        fn remove(self: *Self, slot: *?*Entry) void {
            if (slot.*) |entry| {
                self.bytes -= entry.budget.live + @sizeOf(Entry);
                slot.* = null;
                entry.release();
            }
        }
        fn index(key: [32]u8) usize {
            return (std.mem.readInt(u32, key[0..4], .little) % (capacity / ways)) * ways;
        }
        pub fn lease(self: *Self, key: [32]u8, ttl: u64) ?Lease {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            const start = index(key);
            for (self.slots[start..][0..ways], start..) |item, i| {
                const entry = item orelse continue;
                if (!std.mem.eql(u8, &key, &entry.key)) continue;
                if (!entry.times.valid(std.Io.Clock.real.now(self.io).toSeconds(), ttl)) {
                    self.remove(&self.slots[i]);
                    return null;
                }
                self.stamp +%= 1;
                entry.stamp = self.stamp;
                _ = entry.refs.fetchAdd(1, .monotonic);
                return .{ .entry = entry, .times = entry.times, .value = entry.value, .token = entry.token };
            }
            return null;
        }
        pub fn get(self: *Self, a: Allocator, key: [32]u8, ttl: u64) !?Hit {
            const pinned = self.lease(key, ttl) orelse return null;
            defer pinned.release();
            return .{ .times = pinned.times, .value = try retainedClone(Value, a, pinned.value, 0) };
        }
        pub fn put(self: *Self, key: [32]u8, times: Times, value: Value) void {
            self.mutex.lockUncancelable(self.io);
            defer self.mutex.unlock(self.io);
            // Keep one entry's headroom permanently available. Construction,
            // resident entries and evicted active leases all share this cap.
            // If leases occupy headroom, decline admission without eviction.
            if (limit -| self.physical.load(.monotonic) < reservation) return;
            const entry = self.a.create(Entry) catch return;
            _ = self.physical.fetchAdd(reservation, .monotonic);
            entry.* = .{ .owner = self, .key = key, .token = undefined, .times = times, .budget = .{ .backing = self.a, .limit = limit / 4 }, .arena = undefined, .value = undefined };
            entry.arena = .init(entry.budget.allocator());
            entry.value = retainedClone(Value, entry.arena.allocator(), value, 0) catch {
                entry.arena.deinit();
                self.a.destroy(entry);
                _ = self.physical.fetchSub(reservation, .monotonic);
                return;
            };
            _ = self.physical.fetchSub(limit / 4 - entry.budget.live, .monotonic);
            self.io.random(&entry.token);
            self.stamp +%= 1;
            entry.stamp = self.stamp;
            // Commit only a fully constructed value. Failed admission leaves
            // every existing key, including a same-key replacement, intact.
            const start = index(key);
            var victim = start;
            for (self.slots[start..][0..ways], start..) |item, i| {
                if (item == null or std.mem.eql(u8, &item.?.key, &key)) {
                    victim = i;
                    break;
                }
                if (item.?.stamp < self.slots[victim].?.stamp) victim = i;
            }
            self.remove(&self.slots[victim]);
            const size = entry.budget.live + @sizeOf(Entry);
            while (self.bytes + size > limit - reservation) {
                self.remove(&self.slots[self.clock % capacity]);
                self.clock +%= 1;
            }
            self.bytes += size;
            self.slots[victim] = entry;
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

test "failed admission preserves hot collisions and leases survive eviction" {
    const C = Cache([]const u8, 4, 16384);
    var cache: C = .{ .a = std.testing.allocator, .io = std.testing.io };
    defer cache.deinit();
    var times: Times = .{};
    times.add(std.Io.Clock.real.now(std.testing.io).toSeconds());
    cache.put(@splat(0), times, "hot");
    const pinned = cache.lease(@splat(0), 100).?;
    defer pinned.release();
    const oversized: [8192]u8 = @splat('x');
    cache.put(@splat(0), times, &oversized);
    try std.testing.expect(cache.lease(@splat(1), 100) == null);
    const hot = cache.lease(@splat(0), 100).?;
    hot.release();
    for (1..12) |n| cache.put(@splat(@intCast(n)), times, "new");
    try std.testing.expectEqualStrings("hot", pinned.value);
    try std.testing.expect(cache.physical.load(.monotonic) <= 16384);
    cache.deinit();
    try std.testing.expectEqualStrings("hot", pinned.value);
}

test "associative cache preserves active colliding questions and failed foreign admission" {
    const C = Cache([]const u8, 32, 65536);
    var cache: C = .{ .a = std.testing.allocator, .io = std.testing.io };
    defer cache.deinit();
    var times: Times = .{};
    times.add(std.Io.Clock.real.now(std.testing.io).toSeconds());
    for (0..4) |n| {
        var key: [32]u8 = @splat(0);
        key[0] = @intCast(n * 8);
        cache.put(key, times, "active");
    }
    const first = cache.lease(@splat(0), 100).?;
    first.release();
    var rejected: [32]u8 = @splat(0);
    rejected[0] = 32;
    const oversized: [32768]u8 = @splat('x');
    cache.put(rejected, times, &oversized);
    for (0..4) |n| {
        var key: [32]u8 = @splat(0);
        key[0] = @intCast(n * 8);
        const pinned = cache.lease(key, 100).?;
        pinned.release();
    }
    const recent = cache.lease(@splat(0), 100).?;
    recent.release();
    cache.put(rejected, times, "new");
    const kept = cache.lease(@splat(0), 100).?;
    kept.release();
    var evicted: [32]u8 = @splat(0);
    evicted[0] = 8;
    try std.testing.expect(cache.lease(evicted, 100) == null);
}
