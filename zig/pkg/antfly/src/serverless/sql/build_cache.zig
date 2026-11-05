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

//! Optional worker residency. Leases never wait: a busy entry falls back to a
//! task-local build. No task allocator, spill manager, or match bitmap survives.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Hash = local.sql_operators.HashJoin;
pub const Cache = struct {
    a: A,
    bytes: usize,
    mutex: std.atomic.Mutex = .unlocked,
    entries: [4]?*Entry = @splat(null),
    clock: u64 = 0,
    hits: u64 = 0,
    misses: u64 = 0,
    rejected: [8]?[32]u8 = @splat(null),
    rejected_hand: usize = 0,
    const Entry = struct {
        key: [32]u8,
        budget: local.sql_memory_budget,
        hash: *Hash,
        used: u64,
        busy: bool = true,
        ready: bool = false,
    };
    pub const Lease = struct {
        cache: *Cache,
        entry: *Entry,
        hit: bool,
        pub fn hash(self: Lease) *Hash {
            return self.entry.hash;
        }
        pub fn complete(self: *Lease) void {
            self.entry.ready = true;
        }
        pub fn release(self: *Lease) void {
            while (!self.cache.mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.cache.mutex.unlock();
            self.entry.busy = false;
            if (!self.entry.ready) {
                self.cache.rejected[self.cache.rejected_hand] = self.entry.key;
                self.cache.rejected_hand = (self.cache.rejected_hand + 1) % self.cache.rejected.len;
                for (&self.cache.entries) |*slot| if (slot.* == self.entry) {
                    self.cache.destroy(self.entry);
                    slot.* = null;
                    break;
                };
            }
        }
    };
    fn destroy(self: *Cache, entry: *Entry) void {
        entry.hash.deinit();
        self.a.destroy(entry);
    }
    pub fn deinit(self: *Cache) void {
        for (&self.entries) |*slot| if (slot.*) |entry| {
            std.debug.assert(!entry.busy);
            self.destroy(entry);
            slot.* = null;
        };
    }
    pub fn acquire(self: *Cache, key: [32]u8, task_bytes: usize, rows: usize) ?Lease {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        for (self.rejected) |rejected| if (rejected) |digest| {
            if (std.mem.eql(u8, &digest, &key)) return null;
        };
        self.clock +%= 1;
        var victim: ?usize = null;
        for (self.entries, 0..) |slot, index| {
            if (slot) |entry| {
                if (std.mem.eql(u8, &entry.key, &key)) {
                    if (entry.busy or !entry.ready or entry.budget.live > task_bytes or entry.hash.buildKeys().len() > rows) return null;
                    entry.busy = true;
                    entry.used = self.clock;
                    self.hits +%= 1;
                    return .{ .cache = self, .entry = entry, .hit = true };
                }
                if (!entry.busy and (victim == null or (self.entries[victim.?] != null and entry.used < self.entries[victim.?].?.used))) victim = index;
            } else victim = index;
        }
        self.misses +%= 1;
        const index = victim orelse return null;
        if (self.entries[index]) |old| self.destroy(old);
        self.entries[index] = null;
        const limit = @min(task_bytes, self.bytes / self.entries.len);
        if (limit < 8192) return null;
        const entry = self.a.create(Entry) catch return null;
        entry.* = .{ .key = key, .budget = .{ .backing = self.a, .limit = limit }, .hash = undefined, .used = self.clock };
        entry.hash = Hash.create(entry.budget.allocator(), .{ .bytes = limit, .rows = rows }) catch {
            self.a.destroy(entry);
            return null;
        };
        self.entries[index] = entry;
        return .{ .cache = self, .entry = entry, .hit = false };
    }
};

test "distributed SQL build residency isolates keys rejects busy leases and discards incomplete builds" {
    const a = std.testing.allocator;
    var cache: Cache = .{ .a = a, .bytes = 1024 * 1024 };
    defer cache.deinit();
    var first = cache.acquire(@splat(1), 256 * 1024, 100).?;
    try first.hash().add(&.{local.sql_scalar.Datum.json(.{ .integer = 7 })}, &.{local.sql_scalar.Datum.json(.{ .integer = 7 })});
    try std.testing.expect(cache.acquire(@splat(1), 256 * 1024, 100) == null);
    first.complete();
    first.release();
    var hit = cache.acquire(@splat(1), 256 * 1024, 100).?;
    try std.testing.expect(hit.hit);
    var probe = try hit.hash().probe(&.{local.sql_scalar.Datum.json(.{ .integer = 7 })});
    try std.testing.expect((try probe.next()) != null);
    hit.release();
    var other = cache.acquire(@splat(2), 256 * 1024, 100).?;
    try std.testing.expect(!other.hit);
    other.complete();
    other.release();
    var retained = cache.acquire(@splat(1), 256 * 1024, 100).?;
    try std.testing.expect(retained.hit);
    retained.release();
    var failed = cache.acquire(@splat(3), 256 * 1024, 100).?;
    failed.release();
    try std.testing.expect(cache.acquire(@splat(3), 256 * 1024, 100) == null);
}
