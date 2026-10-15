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

const std = @import("std");

/// A lossy, fixed-size cache of extents discovered by initial validation.
/// Entries borrow immutable source bytes. Collisions only lose an optimization;
/// callers resume ordinary scanning. No document-sized navigation is retained.
pub const ValidatedRanges = struct {
    const Entry = struct { start: usize = 0, len: usize = 0 };
    alloc: std.mem.Allocator,
    entries: []Entry = &.{},
    capacity: usize = 1024,
    complete: bool = false,
    all_keys: bool = false,

    pub fn init(alloc: std.mem.Allocator) @This() {
        return .{ .alloc = @import("antfly_platform").allocator.reclaimingScratch(alloc) };
    }
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.entries);
        self.entries = &.{};
        self.complete = false;
        self.all_keys = false;
    }
    pub fn begin(self: *@This(), input_len: usize) void {
        self.complete = false;
        self.all_keys = false;
        // Small documents need only 1 KiB; large ones stop at 16 KiB.
        const capacity = std.math.ceilPowerOfTwoAssert(usize, @max(64, @min(1024, input_len / 128)));
        if (capacity != self.capacity) {
            self.alloc.free(self.entries);
            self.entries = &.{};
            self.capacity = capacity;
        } else @memset(self.entries, .{});
    }
    pub fn seal(self: *@This(), all_keys: bool) void {
        self.complete = true;
        self.all_keys = all_keys;
    }
    fn index(start: usize, capacity: usize) usize {
        return @intCast(std.hash.Wyhash.hash(0, std.mem.asBytes(&start)) & @as(u64, capacity - 1));
    }
    pub fn remember(self: *@This(), raw: []const u8) !void {
        std.debug.assert(!self.complete);
        if (self.entries.len == 0) {
            const entries = try self.alloc.alloc(Entry, self.capacity);
            @memset(entries, .{});
            self.entries = entries;
        }
        const start = @intFromPtr(raw.ptr);
        self.entries[index(start, self.capacity)] = .{ .start = start, .len = raw.len };
    }
    /// Called only at a value boundary after peekNextTokenType. Structural
    /// shortcuts require the stronger whole-document duplicate-key proof;
    /// scalar shortcuts need only syntax validation (also safe for native JSON).
    pub fn skip(self: *const @This(), scanner: *std.json.Scanner, require_all_keys: bool) !bool {
        if (!self.complete or (require_all_keys and !self.all_keys) or scanner.diagnostics != null) return false;
        const entries = self.entries;
        if (entries.len == 0) return false;
        const start = @intFromPtr(scanner.input.ptr) + scanner.cursor;
        const entry = entries[index(start, self.capacity)];
        if (entry.start != start or entry.len == 0 or entry.len > scanner.input.len - scanner.cursor) return false;
        if (!require_all_keys and (scanner.input[scanner.cursor] == '[' or scanner.input[scanner.cursor] == '{')) return false;
        const input = scanner.input;
        const cursor = scanner.cursor;
        // Let the standard scanner advance its value state and parent stack
        // through a tiny scalar. Only the validated source extent is elided;
        // no private scanner state or stack representation is modified.
        scanner.input = "null";
        scanner.cursor = 0;
        defer scanner.input = input;
        errdefer scanner.cursor = cursor;
        try scanner.skipValue();
        scanner.cursor = cursor + entry.len;
        return true;
    }
};

test "public graph metric borrowed wire validated ranges preserve scanner state and proof boundaries" {
    const a = std.testing.allocator;
    const raw = "{\"x\": [ {\"n\":1} ], \"y\": \"a\\u0062\", \"z\":2}";
    var cache = ValidatedRanges.init(a);
    defer cache.deinit();
    try cache.remember(raw[6..17]);
    try cache.remember(raw[24..33]);
    var scanner = std.json.Scanner.initCompleteInput(a, raw);
    defer scanner.deinit();
    try std.testing.expectEqual(.object_begin, try scanner.next());
    _ = try scanner.next();
    try std.testing.expectEqual(.array_begin, try scanner.peekNextTokenType());
    try std.testing.expect(!try cache.skip(&scanner, true));
    cache.seal(false);
    try std.testing.expect(!try cache.skip(&scanner, true));
    cache.seal(true);
    try std.testing.expect(try cache.skip(&scanner, true));
    try std.testing.expectEqualStrings("y", (try scanner.next()).string);
    try std.testing.expectEqual(.string, try scanner.peekNextTokenType());
    try std.testing.expect(try cache.skip(&scanner, false));
    try std.testing.expectEqualStrings("z", (try scanner.next()).string);
    try std.testing.expectEqualStrings("2", (try scanner.next()).number);
    try std.testing.expectEqual(.object_end, try scanner.next());
    try std.testing.expectEqual(.end_of_document, try scanner.next());
    cache.begin(raw.len);
    try std.testing.expect(!cache.complete);
}

test "public graph metric borrowed wire validated range collisions fall back within fixed storage" {
    const a = std.testing.allocator;
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var cache = ValidatedRanges.init(counted.allocator());
    defer cache.deinit();
    const raw = try a.alloc(u8, 5000 * 2);
    defer a.free(raw);
    for (0..5000) |i| @memcpy(raw[i * 2 ..][0..2], "[]");
    for (0..5000) |i| try cache.remember(raw[i * 2 ..][0..2]);
    cache.seal(true);
    try std.testing.expectEqual(@as(usize, 1), counted.allocations);
    try std.testing.expectEqual(@as(usize, 1024 * @sizeOf(ValidatedRanges.Entry)), counted.allocated_bytes);
    var hits: usize = 0;
    for (0..5000) |i| {
        var scanner = std.json.Scanner.initCompleteInput(a, raw[i * 2 ..][0..2]);
        defer scanner.deinit();
        _ = try scanner.peekNextTokenType();
        if (try cache.skip(&scanner, true)) hits += 1 else try scanner.skipValue();
        try std.testing.expectEqual(.end_of_document, try scanner.next());
    }
    try std.testing.expect(hits > 0 and hits <= 1024);
    cache.deinit();
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}
