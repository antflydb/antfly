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

//! Owner-local original-value certificates. Nothing is persisted or exposed as
//! authorization. Mutation attempts fence admission before backend commit and
//! retire it after success OR ambiguous failure. Unrelated writes retain facts;
//! old read cuts cannot register facts in a newer owner generation.
const std = @import("std");
var nonces: std.atomic.Value(u64) = .init(1);
pub const Token = struct { owner: usize, nonce: u64, epoch: u64 };
pub fn Registry(comptime Content: type) type {
    return struct {
        const Self = @This();
        pub const Facts = struct { content: Content, timestamp: u64 };
        pub const capacity = 4096;
        const Entry = struct { facts: Facts, slot: usize, referenced: bool = true };
        mutex: std.Io.Mutex = .init,
        nonce: u64 = 0,
        epoch: u64 = 0,
        writers: usize = 0,
        disabled: bool = false,
        entries: std.AutoHashMapUnmanaged([32]u8, Entry) = .empty,
        slots: std.ArrayList(?[32]u8) = .empty,
        hand: usize = 0,
        fn clear(self: *Self) void {
            self.entries.clearRetainingCapacity();
            self.slots.clearRetainingCapacity();
            self.hand = 0;
        }
        fn io() std.Io {
            return if (@import("builtin").os.tag == .freestanding) .failing else std.Io.Threaded.global_single_threaded.io();
        }
        fn lock(self: *Self) void {
            self.mutex.lockUncancelable(io());
        }
        fn unlock(self: *Self) void {
            self.mutex.unlock(io());
        }
        fn valid(self: *Self, token: ?Token) bool {
            const t = token orelse return false;
            return !self.disabled and self.writers == 0 and t.owner == @intFromPtr(self) and self.nonce == t.nonce and self.epoch == t.epoch;
        }
        pub fn capture(self: *Self) ?Token {
            self.lock();
            defer self.unlock();
            if (self.disabled or self.writers != 0) return null;
            if (self.nonce == 0) {
                var old = nonces.load(.monotonic);
                while (true) {
                    if (old == std.math.maxInt(u64)) {
                        self.disabled = true;
                        return null;
                    }
                    if (nonces.cmpxchgWeak(old, old + 1, .monotonic, .monotonic)) |actual| old = actual else break;
                }
                self.nonce = old;
            }
            return .{ .owner = @intFromPtr(self), .nonce = self.nonce, .epoch = self.epoch };
        }
        pub fn confirm(self: *Self, token: ?Token) ?Token {
            self.lock();
            defer self.unlock();
            return if (self.valid(token)) token else null;
        }
        pub fn get(self: *Self, token: ?Token, key: [32]u8) ?Facts {
            self.lock();
            defer self.unlock();
            return if (self.valid(token)) self.getLocked(key) else null;
        }
        fn getLocked(self: *Self, key: [32]u8) ?Facts {
            const entry = self.entries.getPtr(key) orelse return null;
            entry.referenced = true;
            return entry.facts;
        }
        pub fn getMany(self: *Self, token: ?Token, keys: []const [32]u8, output: []?Facts) void {
            std.debug.assert(keys.len == output.len);
            self.lock();
            defer self.unlock();
            if (!self.valid(token)) {
                @memset(output, null);
                return;
            }
            for (keys, output) |key, *out| out.* = self.getLocked(key);
        }
        pub fn put(self: *Self, a: std.mem.Allocator, token: ?Token, key: [32]u8, facts: Facts) void {
            self.lock();
            defer self.unlock();
            if (!self.valid(token)) return;
            self.putLocked(a, key, facts);
        }
        fn putLocked(self: *Self, a: std.mem.Allocator, key: [32]u8, facts: Facts) void {
            if (self.entries.getPtr(key)) |entry| {
                entry.facts = facts;
                entry.referenced = true;
                return;
            }
            // Allocate before eviction. OOM never discards an unrelated hit.
            self.entries.ensureUnusedCapacity(a, 1) catch return;
            var slot: usize = undefined;
            if (self.slots.items.len < capacity) {
                slot = self.slots.items.len;
                self.slots.append(a, null) catch return;
            } else {
                // CLOCK: at most two bounded rotations, amortized O(1), no
                // whole-cache cliff when a single cold identity is admitted.
                while (true) {
                    slot = self.hand;
                    self.hand = (self.hand + 1) % capacity;
                    const prior = self.slots.items[slot] orelse break;
                    const entry = self.entries.getPtr(prior).?;
                    if (self.entries.count() < capacity) continue;
                    if (entry.referenced) {
                        entry.referenced = false;
                        continue;
                    }
                    _ = self.entries.remove(prior);
                    break;
                }
            }
            self.slots.items[slot] = key;
            self.entries.putAssumeCapacity(key, .{ .facts = facts, .slot = slot });
        }
        pub fn putMany(self: *Self, a: std.mem.Allocator, token: ?Token, keys: []const [32]u8, facts: []const ?Facts) void {
            std.debug.assert(keys.len == facts.len);
            self.lock();
            defer self.unlock();
            if (!self.valid(token)) return;
            for (keys, facts) |key, fact| if (fact) |value| self.putLocked(a, key, value);
        }

        fn advance(self: *Self) void {
            if (self.epoch == std.math.maxInt(u64)) self.disabled = true else self.epoch += 1;
        }
        pub fn begin(self: *Self, changed: anytype) void {
            self.lock();
            defer self.unlock();
            self.advance();
            self.writers += 1;
            if (changed.count() > capacity) {
                self.clear();
                return;
            }
            var it = changed.keyIterator();
            while (it.next()) |key| {
                if (self.entries.fetchRemove(key.*[key.len - 32 ..].*)) |entry| self.slots.items[entry.value.slot] = null;
            }
        }
        pub fn beginReplacement(self: *Self) void {
            self.lock();
            defer self.unlock();
            self.advance();
            self.writers += 1;
            self.clear();
        }
        pub fn end(self: *Self) void {
            self.lock();
            defer self.unlock();
            std.debug.assert(self.writers > 0);
            self.writers -= 1;
            self.advance();
        }
        pub fn deinit(self: *Self, a: std.mem.Allocator) void {
            self.entries.deinit(a);
            self.slots.deinit(a);
            self.* = .{};
        }
    };
}

test "source certificates retain unrelated facts and fence old cuts and failed attempts" {
    const R = Registry(u64);
    var registry: R = .{};
    defer registry.deinit(std.testing.allocator);
    const a = std.testing.allocator;
    const x: [32]u8 = @splat(1);
    const y: [32]u8 = @splat(2);
    const old = registry.capture();
    registry.put(a, old, x, .{ .content = 10, .timestamp = 4 });
    registry.put(a, old, y, .{ .content = 20, .timestamp = 5 });
    var changed: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer changed.deinit(a);
    try changed.put(a, x, {});
    registry.begin(&changed);
    try std.testing.expect(registry.capture() == null);
    try std.testing.expect(registry.get(old, y) == null);
    registry.put(a, old, x, .{ .content = 999, .timestamp = 0 });
    registry.end(); // includes failed/ambiguous commits: no refund of old proofs
    const fresh = registry.capture();
    try std.testing.expect(registry.get(fresh, x) == null);
    try std.testing.expectEqual(@as(u64, 20), registry.get(fresh, y).?.content);
    registry.put(a, old, x, .{ .content = 999, .timestamp = 0 });
    try std.testing.expect(registry.get(fresh, x) == null);
    registry.beginReplacement();
    registry.end();
    try std.testing.expect(registry.get(registry.capture(), y) == null);
    var second: R = .{};
    defer second.deinit(a);
    _ = second.capture();
    second.put(a, fresh, y, .{ .content = 888, .timestamp = 0 });
    try std.testing.expect(second.get(second.capture(), y) == null);
}

test "source certificates allocation failure and bounded admission are misses" {
    const R = Registry(u64);
    var registry: R = .{};
    defer registry.deinit(std.testing.allocator);
    const token = registry.capture();
    const key: [32]u8 = @splat(0);
    registry.put(std.testing.failing_allocator, token, key, .{ .content = 1, .timestamp = 0 });
    try std.testing.expect(registry.get(token, key) == null);
    for (0..R.capacity + 1) |i| {
        var k: [32]u8 = @splat(0);
        std.mem.writeInt(u64, k[0..8], i, .little);
        registry.put(std.testing.allocator, token, k, .{ .content = i, .timestamp = 0 });
    }
    try std.testing.expect(registry.entries.count() <= R.capacity);
}

test "source certificates overlapping writers and epoch exhaustion fail closed" {
    const R = Registry(u64);
    var r: R = .{};
    defer r.deinit(std.testing.allocator);
    var changed: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer changed.deinit(std.testing.allocator);
    const before = r.capture();
    r.begin(&changed);
    r.begin(&changed);
    r.end();
    try std.testing.expect(r.capture() == null);
    r.end();
    try std.testing.expect(r.confirm(before) == null);
    r.epoch = std.math.maxInt(u64);
    r.begin(&changed);
    r.end();
    try std.testing.expect(r.capture() == null);
}

test "source certificates concurrent registration cannot cross a mutation fence" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const R = Registry(u64);
    var r: R = .{};
    defer r.deinit(std.testing.allocator);
    const key: [32]u8 = @splat(7);
    const token = r.capture();
    r.put(std.testing.allocator, token, key, .{ .content = 1, .timestamp = 0 });
    const Context = struct {
        registry: *R,
        begun: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        fn writer(self: *@This()) void {
            self.registry.beginReplacement();
            self.begun.store(true, .release);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
            self.registry.end();
        }
    };
    var context: Context = .{ .registry = &r };
    const writer = try std.Thread.spawn(.{}, Context.writer, .{&context});
    defer {
        context.release.store(true, .release);
        writer.join();
    }
    while (!context.begun.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expect(r.capture() == null);
    r.put(std.testing.allocator, token, key, .{ .content = 999, .timestamp = 0 });
    try std.testing.expect(r.get(token, key) == null);
}

test "source certificates CLOCK retains the hot working set and reuses mutation holes" {
    const R = Registry(u64);
    const a = std.testing.allocator;
    var r: R = .{};
    defer r.deinit(a);
    const Keys = struct {
        fn key(n: u64) [32]u8 {
            var k: [32]u8 = @splat(0);
            std.mem.writeInt(u64, k[0..8], n, .little);
            return k;
        }
    };
    const token = r.capture();
    for (0..R.capacity) |i| r.put(a, token, Keys.key(i), .{ .content = i, .timestamp = 0 });
    r.put(a, token, Keys.key(R.capacity), .{ .content = 42, .timestamp = 0 });
    var hits: usize = 0;
    for (0..R.capacity) |i| hits += @intFromBool(r.get(token, Keys.key(i)) != null);
    try std.testing.expectEqual(R.capacity - 1, hits);
    var changed: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer changed.deinit(a);
    try changed.put(a, Keys.key(1), {});
    r.begin(&changed);
    r.end();
    const fresh = r.capture();
    r.put(a, fresh, Keys.key(R.capacity + 1), .{ .content = 43, .timestamp = 0 });
    try std.testing.expectEqual(R.capacity, r.entries.count());
    const keys = [_][32]u8{ Keys.key(2), Keys.key(R.capacity + 1) };
    var facts: [2]?R.Facts = undefined;
    r.getMany(fresh, &keys, &facts);
    try std.testing.expectEqual(@as(u64, 2), facts[0].?.content);
    try std.testing.expectEqual(@as(u64, 43), facts[1].?.content);
    r.getMany(token, &keys, &facts);
    try std.testing.expect(facts[0] == null and facts[1] == null);
}
