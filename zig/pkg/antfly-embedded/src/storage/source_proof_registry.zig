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
        mutex: std.Io.Mutex = .init,
        nonce: u64 = 0,
        epoch: u64 = 0,
        writers: usize = 0,
        disabled: bool = false,
        entries: std.AutoHashMapUnmanaged([32]u8, Facts) = .empty,
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
            return if (self.valid(token)) self.entries.get(key) else null;
        }
        pub fn put(self: *Self, a: std.mem.Allocator, token: ?Token, key: [32]u8, facts: Facts) void {
            self.lock();
            defer self.unlock();
            if (!self.valid(token)) return;
            // Fixed entry admission; no unbounded registration fan-out or
            // per-source allocation. Pressure evicts facts, never correctness.
            if (self.entries.count() >= capacity and !self.entries.contains(key)) self.entries.clearRetainingCapacity();
            self.entries.put(a, key, facts) catch {};
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
                self.entries.clearRetainingCapacity();
                return;
            }
            var it = changed.keyIterator();
            while (it.next()) |key| _ = self.entries.remove(key.*[key.len - 32 ..].*);
        }
        pub fn beginReplacement(self: *Self) void {
            self.lock();
            defer self.unlock();
            self.advance();
            self.writers += 1;
            self.entries.clearRetainingCapacity();
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
