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

//! Document-scoped reuse of identical analysis. Only repeated contributions
//! retain tokens, in independent regions released after the last binding.
const std = @import("std");
const analysis = @import("../search/analysis.zig");
const A = std.mem.Allocator;
pub const Cache = struct {
    pub const Key = struct { field: []const u8, text: []const u8, signature: [32]u8 };
    const Context = struct {
        pub fn hash(_: @This(), key: Key) u64 {
            return std.hash.Wyhash.hash(std.hash.Wyhash.hash(std.hash.Wyhash.hash(0, &key.signature), key.field), key.text);
        }
        pub fn eql(_: @This(), l: Key, r: Key) bool {
            return std.mem.eql(u8, &l.signature, &r.signature) and std.mem.eql(u8, l.field, r.field) and std.mem.eql(u8, l.text, r.text);
        }
    };
    const Entry = struct { remaining: usize = 0, owner: ?*std.heap.ArenaAllocator = null, tokens: ?[]analysis.Token = null };
    pub const Lease = struct {
        a: A,
        entry: *Entry,
        tokens: []analysis.Token,
        pub fn release(self: Lease) void {
            std.debug.assert(self.entry.remaining > 0);
            self.entry.remaining -= 1;
            if (self.entry.remaining == 0) {
                self.entry.owner.?.deinit();
                self.a.destroy(self.entry.owner.?);
                self.entry.owner = null;
                self.entry.tokens = null;
            }
        }
    };
    a: A,
    entries: std.HashMapUnmanaged(Key, Entry, Context, 80) = .empty,
    analyses: usize = 0,
    reuses: usize = 0,
    pub fn init(a: A) Cache {
        return .{ .a = @import("../sql/memory_budget.zig").reclaimingAllocator(a) };
    }
    pub fn deinit(self: *Cache) void {
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry| if (entry.owner) |owner| {
            owner.deinit();
            self.a.destroy(owner);
        };
        self.entries.deinit(self.a);
    }
    /// Finish discovery before acquiring a lease: entries then remain stable.
    pub fn note(self: *Cache, key: Key) !void {
        const result = try self.entries.getOrPut(self.a, key);
        if (!result.found_existing) result.value_ptr.* = .{};
        result.value_ptr.remaining += 1;
    }
    pub fn acquire(self: *Cache, key: Key, analyzer: *const analysis.Analyzer, offsets: bool) !?Lease {
        const entry = self.entries.getPtr(key) orelse return null;
        if (entry.remaining == 1 and entry.tokens == null) return null;
        if (entry.tokens == null) {
            const owner = try self.a.create(std.heap.ArenaAllocator);
            owner.* = .init(self.a);
            errdefer {
                owner.deinit();
                self.a.destroy(owner);
            }
            const tokens = if (offsets) try analyzer.analyzeWithSourceOffsets(owner.allocator(), key.text) else try analyzer.analyze(owner.allocator(), key.text);
            entry.owner = owner;
            entry.tokens = tokens;
            self.analyses += 1;
        } else self.reuses += 1;
        return .{ .a = self.a, .entry = entry, .tokens = entry.tokens.? };
    }
};

test "highlight indexing analysis reuse releases last leases and fences source and pipeline under allocation faults" {
    const Run = struct {
        fn run(a: A) !void {
            var cache = Cache.init(a);
            defer cache.deinit();
            const key: Cache.Key = .{ .field = "body", .text = "Running needles", .signature = @splat(3) };
            try cache.note(key);
            try cache.note(key);
            const first = (try cache.acquire(key, &analysis.simple_analyzer, true)).?;
            const ptr = first.tokens.ptr;
            first.release();
            const second = (try cache.acquire(key, &analysis.simple_analyzer, true)).?;
            try std.testing.expect(ptr == second.tokens.ptr);
            try std.testing.expectEqual(@as(usize, 1), cache.analyses);
            try std.testing.expectEqual(@as(usize, 1), cache.reuses);
            second.release();
            try std.testing.expect(cache.entries.get(key).?.owner == null);
            var other = key;
            other.field = "different";
            try std.testing.expect((try cache.acquire(other, &analysis.simple_analyzer, true)) == null);
            other = key;
            other.signature = @splat(4);
            try std.testing.expect((try cache.acquire(other, &analysis.simple_analyzer, true)) == null);
        }
    };
    try Run.run(std.testing.allocator);
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Run.run, .{});
}
