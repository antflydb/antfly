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

//! A retained owner cut may memoize producer visibility, never mutable rows.
//! Fixed storage bounds both entry count and key bytes. Oversized keys still
//! use reclaimable scratch, but are deliberately not retained in the memo.
const std = @import("std");
const b = @import("backend.zig");
pub const Memo = struct {
    const Entry = struct { occupied: bool = false, hash: u64 = 0, len: usize = 0, key: [256]u8 = undefined, allowed: bool = false };
    // Four-way sets keep lookup work independent of the retained producer
    // count, without a heap-owned hash table or unbounded key storage.
    entries: [32][4]Entry = @splat(@splat(.{})),
    next: [32]u2 = @splat(0),
    pub fn allows(self: *Memo, a: std.mem.Allocator, key: []const u8, context: anytype, comptime read: anytype) !bool {
        const hash = std.hash.Wyhash.hash(0, key);
        const set = hash % self.entries.len;
        if (key.len <= 256) for (&self.entries[set]) |*entry| {
            if (entry.occupied and entry.hash == hash and std.mem.eql(u8, entry.key[0..entry.len], key)) return entry.allowed;
        };
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const allowed = try read(context, scratch.allocator(), key);
        if (key.len <= 256) {
            const entry = &self.entries[set][self.next[set]];
            entry.occupied = true;
            entry.hash = hash;
            entry.len = key.len;
            @memcpy(entry.key[0..key.len], key);
            entry.allowed = allowed;
            self.next[set] +%= 1;
        }
        return allowed;
    }
};

test "producer visibility reclaims wide parsing scratch and bounds retained decisions" {
    const Fixture = struct {
        calls: usize = 0,
        fail: bool = false,
        fn read(self: *@This(), a: std.mem.Allocator, key: []const u8) !bool {
            self.calls += 1;
            if (self.fail) return error.Forbidden;
            const json = try a.alloc(u8, (1 << 20) + 2);
            json[0] = '"';
            @memset(json[1 .. json.len - 1], 'x');
            json[json.len - 1] = '"';
            _ = try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .allocate = .alloc_always });
            return !std.mem.eql(u8, key, "hidden");
        }
    };
    var budget: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = 8 << 20 };
    var retained = std.heap.ArenaAllocator.init(budget.allocator());
    defer retained.deinit();
    var memo: Memo = .{};
    var fixture: Fixture = .{};
    for (0..128) |_| try std.testing.expect(try memo.allows(retained.allocator(), "producer", &fixture, Fixture.read));
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    for (0..128) |_| try std.testing.expect(!try memo.allows(retained.allocator(), "hidden", &fixture, Fixture.read));
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    // Uncached long keys and evictions cannot accumulate JSON buffers in the
    // reply arena, even when every lookup needs a fresh authorization read.
    const long: [257]u8 = @splat('p');
    for (0..16) |_| try std.testing.expect(try memo.allows(retained.allocator(), &long, &fixture, Fixture.read));
    for (0..129) |index| {
        var key: [32]u8 = undefined;
        try std.testing.expect(try memo.allows(retained.allocator(), try std.fmt.bufPrint(&key, "producer-{d}", .{index}), &fixture, Fixture.read));
    }
    var collisions: usize = 0;
    var candidate: usize = 0;
    while (collisions < 4) : (candidate += 1) {
        var key: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&key, "collision-{d}", .{candidate});
        if (std.hash.Wyhash.hash(0, value) % 32 != std.hash.Wyhash.hash(0, "producer") % 32) continue;
        try std.testing.expect(try memo.allows(retained.allocator(), value, &fixture, Fixture.read));
        collisions += 1;
    }
    const before = fixture.calls;
    try std.testing.expect(try memo.allows(retained.allocator(), "producer", &fixture, Fixture.read));
    try std.testing.expectEqual(before + 1, fixture.calls);
    fixture.fail = true;
    try std.testing.expectError(error.Forbidden, memo.allows(retained.allocator(), "revoked", &fixture, Fixture.read));
    fixture.fail = false;
    try std.testing.expect(try memo.allows(retained.allocator(), "revoked", &fixture, Fixture.read));
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak < 8 << 20);
}
