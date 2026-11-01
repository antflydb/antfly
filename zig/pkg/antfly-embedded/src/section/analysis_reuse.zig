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
//! retain normalized replay or tokens, released after the last binding.
const std = @import("std");
const analysis = @import("../search/analysis.zig");
const A = std.mem.Allocator;
const Spool = @import("source_token_spool.zig").Spool;
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
    const Entry = struct { remaining: usize = 0, owner: ?*std.heap.ArenaAllocator = null, tokens: ?[]analysis.Token = null, replay: ?*Spool = null };
    pub const Lease = struct {
        a: A,
        entry: *Entry,
        tokens: []analysis.Token,
        pub fn release(self: Lease) void {
            releaseEntry(self.a, self.entry);
        }
    };
    /// A document-local lease owns one normalized stream through every alias.
    /// The first consumer receives tokens directly while the replay is built;
    /// later consumers replay compact records without analyzing again.
    pub const ReplayLease = struct {
        a: A,
        entry: *Entry,
        analyzer: *const analysis.Analyzer,
        text: []const u8,
        fresh: bool,
        pub fn visit(self: ReplayLease, consumer: anytype) !Spool.Input {
            const spool = self.entry.replay.?;
            if (self.fresh) {
                var stream = self.analyzer.sourceStream(self.a, self.text).?;
                defer stream.deinit();
                while (try stream.next()) |token| {
                    try consumer.accept(consumer.ptr, token);
                    try spool.append(token);
                }
            } else {
                const input = try spool.input();
                for (0..input.count) |i| try consumer.accept(consumer.ptr, try input.at(i));
            }
            return spool.input();
        }
        pub fn release(self: ReplayLease) void {
            releaseEntry(self.a, self.entry);
        }
    };
    fn clearEntry(a: A, entry: *Entry) void {
        if (entry.owner) |owner| {
            owner.deinit();
            a.destroy(owner);
            entry.owner = null;
            entry.tokens = null;
        }
        if (entry.replay) |spool| {
            spool.deinit();
            a.destroy(spool);
            entry.replay = null;
        }
    }
    fn releaseEntry(a: A, entry: *Entry) void {
        std.debug.assert(entry.remaining > 0);
        entry.remaining -= 1;
        if (entry.remaining == 0) clearEntry(a, entry);
    }
    a: A,
    entries: std.HashMapUnmanaged(Key, Entry, Context, 80) = .empty,
    analyses: usize = 0,
    reuses: usize = 0,
    pub fn init(a: A) Cache {
        return .{ .a = @import("../sql/memory_budget.zig").reclaimingAllocator(a) };
    }
    pub fn deinit(self: *Cache) void {
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry| clearEntry(self.a, entry);
        self.entries.deinit(self.a);
    }
    /// Finish discovery before acquiring a lease: entries then remain stable.
    pub fn note(self: *Cache, key: Key) !void {
        const result = try self.entries.getOrPut(self.a, key);
        if (!result.found_existing) result.value_ptr.* = .{};
        result.value_ptr.remaining += 1;
    }
    pub fn repeated(self: *const Cache, key: Key) bool {
        const entry = self.entries.get(key) orelse return false;
        return entry.remaining > 1 or entry.tokens != null or entry.replay != null;
    }
    pub fn hasReplay(self: *const Cache, key: Key) bool {
        return if (self.entries.get(key)) |entry| entry.replay != null else false;
    }
    pub fn acquireReplay(self: *Cache, key: Key, analyzer: *const analysis.Analyzer, io: ?std.Io, directory: []const u8, resources: ?*@import("../storage/resource_manager.zig").ResourceManager) !?ReplayLease {
        const entry = self.entries.getPtr(key) orelse return null;
        if (entry.tokens != null or (entry.remaining < 2 and entry.replay == null)) return null;
        const fresh = entry.replay == null;
        if (fresh) {
            var eligibility = analyzer.sourceStream(self.a, key.text) orelse return null;
            eligibility.deinit();
            const spool = try self.a.create(Spool);
            spool.* = .init(self.a, io, directory, resources);
            entry.replay = spool;
            self.analyses += 1;
        } else self.reuses += 1;
        return .{ .a = self.a, .entry = entry, .analyzer = analyzer, .text = key.text, .fresh = fresh };
    }
    pub fn acquire(self: *Cache, key: Key, analyzer: *const analysis.Analyzer, offsets: bool) !?Lease {
        const entry = self.entries.getPtr(key) orelse return null;
        std.debug.assert(entry.replay == null);
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

test "highlight compact analysis replay leases preserve alias tokens and retire the final disk owner under faults" {
    const Check = struct {
        const Consumer = struct {
            expected: []const analysis.Token,
            at: usize = 0,
            fn accept(raw: *anyopaque, token: analysis.Token) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try std.testing.expect(self.at < self.expected.len);
                const expected = self.expected[self.at];
                try std.testing.expectEqualStrings(expected.term, token.term);
                try std.testing.expectEqual(expected.position, token.position);
                try std.testing.expectEqual(expected.start_byte, token.start_byte);
                try std.testing.expectEqual(expected.end_byte, token.end_byte);
                self.at += 1;
            }
        };
        fn run(a: A, disk: bool) !void {
            const text = "The running dogs café quick foxes needle tail";
            var reference = std.heap.ArenaAllocator.init(a);
            defer reference.deinit();
            const expected = try analysis.default_analyzer.analyzeWithSourceOffsets(reference.allocator(), text);
            var cache = Cache.init(a);
            defer cache.deinit();
            const key: Cache.Key = .{ .field = "body", .text = text, .signature = @splat(7) };
            for (0..3) |_| try cache.note(key);
            for (0..3) |index| {
                const held = (try cache.acquireReplay(key, &analysis.default_analyzer, if (disk) std.testing.io else null, ".", null)).?;
                defer held.release();
                if (index == 0) held.entry.replay.?.max_memory_bytes = 16;
                var consumer: Consumer = .{ .expected = expected };
                const input = try held.visit(.{ .ptr = @as(*anyopaque, &consumer), .accept = Consumer.accept });
                try std.testing.expectEqual(expected.len, input.count);
                try std.testing.expectEqual(expected.len, consumer.at);
                try std.testing.expectEqual(disk, held.entry.replay.?.run != null);
                try std.testing.expect(held.entry.tokens == null and held.entry.owner == null);
            }
            try std.testing.expectEqual(@as(usize, 1), cache.analyses);
            try std.testing.expectEqual(@as(usize, 2), cache.reuses);
            try std.testing.expect(!cache.hasReplay(key));
        }
    };
    for ([_]bool{ false, true }) |disk| {
        try Check.run(std.testing.allocator, disk);
        var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
        try std.testing.checkAllAllocationFailures(stable.allocator(), Check.run, .{disk});
    }
}
