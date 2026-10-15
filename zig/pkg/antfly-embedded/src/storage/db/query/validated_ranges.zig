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

/// Shared, bounded lookaside storage. Entries contain syntax-validated extents,
/// not authority to bypass duplicate checks. Each borrowing view carries its
/// own immutable proof over pinned source bytes. Eviction only loses speed.
pub const ValidatedRanges = struct {
    const Entry = struct { start: usize = 0, len: usize = 0 };
    alloc: std.mem.Allocator,
    entries: []Entry = &.{},
    capacity: usize = 64,
    scope_ids: []u32 = &.{},
    scopes: @import("validation_scopes.zig").Scopes = .{},
    primary: ?Ref = null,
    additional: ?*Certificate = null,
    const Certificate = struct { value: Ref, next: ?*Certificate };

    pub const Proof = struct {
        source: []const u8,
        all_keys: bool,
        fn contains(self: @This(), raw: []const u8) bool {
            const base = @intFromPtr(self.source.ptr);
            const start = @intFromPtr(raw.ptr);
            return start >= base and start - base <= self.source.len and raw.len <= self.source.len - (start - base);
        }
    };
    pub const Ref = struct {
        cache: *ValidatedRanges,
        proof: Proof,

        /// The scanner must be at a value boundary after peekNextTokenType.
        /// Structural shortcuts require full-key validation of this source;
        /// native visited-only proofs can authorize scalar syntax reuse.
        pub fn skip(self: @This(), scanner: *std.json.Scanner, require_all_keys: bool) !bool {
            if ((require_all_keys and !self.proof.all_keys) or scanner.diagnostics != null) return false;
            const cache = self.cache;
            if (cache.entries.len == 0) return false;
            const start = @intFromPtr(scanner.input.ptr) + scanner.cursor;
            const entry = cache.entries[index(start, cache.capacity)];
            if (entry.start != start or entry.len == 0 or entry.len > scanner.input.len - scanner.cursor) return false;
            const raw = scanner.input[scanner.cursor..][0..entry.len];
            if (!self.proof.contains(raw)) return false;
            if (!require_all_keys and (raw[0] == '[' or raw[0] == '{')) return false;
            return advance(scanner, entry.len);
        }
        /// Array extents validated under visited-key policy need a contract
        /// covering this exact requested subtree. IDs reference owned snapshots,
        /// and source containment prevents sharing authority between columns.
        pub fn skipSelected(self: @This(), scanner: *std.json.Scanner, required: *const @import("json_projection_plan.zig").Selector) !bool {
            if (required.fields.count() == 0) return false;
            if (self.proof.all_keys) return self.skip(scanner, true);
            const cache = self.cache;
            if (scanner.diagnostics != null or cache.entries.len == 0 or cache.scope_ids.len == 0) return false;
            const start = @intFromPtr(scanner.input.ptr) + scanner.cursor;
            const slot = index(start, cache.capacity);
            const entry = cache.entries[slot];
            if (entry.start != start or entry.len == 0 or entry.len > scanner.input.len - scanner.cursor or !self.proof.contains(scanner.input[scanner.cursor..][0..entry.len])) return false;
            if (!cache.scopes.covers(cache.scope_ids[slot], required)) return false;
            return advance(scanner, entry.len);
        }
        fn advance(scanner: *std.json.Scanner, len: usize) !bool {
            const input = scanner.input;
            const cursor = scanner.cursor;
            // Let the standard scanner update its value and parent state.
            scanner.input = "null";
            scanner.cursor = 0;
            defer scanner.input = input;
            errdefer scanner.cursor = cursor;
            try scanner.skipValue();
            scanner.cursor = cursor + len;
            return true;
        }
        /// Refill only after a fallback has consumed a complete value. This
        /// never allocates and cannot expand a proof's input or key policy.
        pub fn refill(self: @This(), raw: []const u8) void {
            if (self.cache.entries.len == 0 or raw.len == 0 or !self.proof.contains(raw)) return;
            if ((raw[0] == '[' or raw[0] == '{') and !self.proof.all_keys) return;
            self.cache.store(raw);
        }
        pub fn refillSelected(self: @This(), raw: []const u8, selected: *const @import("json_projection_plan.zig").Selector) !void {
            if (self.cache.entries.len == 0 or !self.proof.contains(raw)) return;
            if (self.proof.all_keys) return self.refill(raw);
            try self.cache.rememberSelected(raw, selected);
        }
    };

    pub fn init(alloc: std.mem.Allocator) @This() {
        return .{ .alloc = @import("antfly_platform").allocator.reclaimingScratch(alloc) };
    }
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.entries);
        self.entries = &.{};
        self.alloc.free(self.scope_ids);
        self.scope_ids = &.{};
        self.scopes.deinit(self.alloc);
        while (self.additional) |certificate| {
            self.additional = certificate.next;
            self.alloc.destroy(certificate);
        }
        self.primary = null;
    }
    /// One response shares at most 16 KiB, even across multiple JSON columns.
    /// Grow the hint only when needed; existing proofs survive cache eviction.
    pub fn reserveFor(self: *@This(), input_len: usize) void {
        const capacity = std.math.ceilPowerOfTwoAssert(usize, @max(64, @min(1024, input_len / 128)));
        if (capacity > self.capacity) {
            self.alloc.free(self.entries);
            self.entries = &.{};
            self.alloc.free(self.scope_ids);
            self.scope_ids = &.{};
            self.capacity = capacity;
        }
    }
    fn index(start: usize, capacity: usize) usize {
        return @intCast(std.hash.Wyhash.hash(0, std.mem.asBytes(&start)) & @as(u64, capacity - 1));
    }
    fn store(self: *@This(), raw: []const u8) void {
        const start = @intFromPtr(raw.ptr);
        const slot = index(start, self.capacity);
        self.entries[slot] = .{ .start = start, .len = raw.len };
        if (self.scope_ids.len != 0) self.scope_ids[slot] = 0;
    }
    pub fn remember(self: *@This(), raw: []const u8) !void {
        if (self.entries.len == 0) {
            const entries = try self.alloc.alloc(Entry, self.capacity);
            @memset(entries, .{});
            self.entries = entries;
        }
        self.store(raw);
    }
    /// Small extents cannot repay contract capture. Keep ordinary shallow and
    /// tiny native projections allocation-free when they need no syntax cache.
    pub fn rememberSelected(self: *@This(), raw: []const u8, selected: *const @import("json_projection_plan.zig").Selector) !void {
        if (raw.len < 256 or raw[0] != '[') return;
        const id = (try self.scopes.intern(self.alloc, selected)) orelse return;
        if (id == 0) return;
        if (self.scope_ids.len == 0) {
            self.scope_ids = try self.alloc.alloc(u32, self.capacity);
            @memset(self.scope_ids, 0);
        }
        try self.remember(raw);
        self.scope_ids[index(@intFromPtr(raw.ptr), self.capacity)] = id;
    }
    /// Publish only after complete syntax and the requested duplicate-key
    /// validation succeeds. Empty caches add no work to the emission path.
    pub fn validated(self: *@This(), raw: []const u8, all_keys: bool) ?Ref {
        return if (self.entries.len == 0) null else .{ .cache = self, .proof = .{ .source = raw, .all_keys = all_keys } };
    }
    /// Stable certificates share the cache lifetime. The usual single-source
    /// response needs no certificate allocation; additional JSON columns get
    /// independently owned records so publishing never mutates an older proof.
    pub fn publish(self: *@This(), raw: []const u8, all_keys: bool) !?*const Ref {
        const value = self.validated(raw, all_keys) orelse return null;
        if (self.primary == null) {
            self.primary = value;
            return &self.primary.?;
        }
        const certificate = try self.alloc.create(Certificate);
        certificate.* = .{ .value = value, .next = self.additional };
        self.additional = certificate;
        return &certificate.value;
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
    const visited = cache.validated(raw, false).?;
    try std.testing.expect(!try visited.skip(&scanner, true));
    const checked = cache.validated(raw, true).?;
    try std.testing.expect(try checked.skip(&scanner, true));
    try std.testing.expectEqualStrings("y", (try scanner.next()).string);
    try std.testing.expectEqual(.string, try scanner.peekNextTokenType());
    try std.testing.expect(try checked.skip(&scanner, false));
    try std.testing.expectEqualStrings("z", (try scanner.next()).string);
    try std.testing.expectEqualStrings("2", (try scanner.next()).number);
    try std.testing.expectEqual(.object_end, try scanner.next());
    try std.testing.expectEqual(.end_of_document, try scanner.next());
    cache.deinit();
    try std.testing.expect(!try checked.skip(&scanner, false));
}

test "public graph metric borrowed wire validated range collisions fall back within fixed storage" {
    const a = std.testing.allocator;
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var cache = ValidatedRanges.init(counted.allocator());
    defer cache.deinit();
    const raw = try a.alloc(u8, 5000 * 2);
    defer a.free(raw);
    for (0..5000) |i| @memcpy(raw[i * 2 ..][0..2], "[]");
    cache.reserveFor(128 * 1024);
    for (0..5000) |i| try cache.remember(raw[i * 2 ..][0..2]);
    const checked = cache.validated(raw, true).?;
    try std.testing.expectEqual(@as(usize, 1), counted.allocations);
    try std.testing.expectEqual(@as(usize, 1024 * @sizeOf(ValidatedRanges.Entry)), counted.allocated_bytes);
    var hits: usize = 0;
    for (0..5000) |i| {
        var scanner = std.json.Scanner.initCompleteInput(a, raw[i * 2 ..][0..2]);
        defer scanner.deinit();
        _ = try scanner.peekNextTokenType();
        if (try checked.skip(&scanner, true)) hits += 1 else try scanner.skipValue();
        try std.testing.expectEqual(.end_of_document, try scanner.next());
    }
    try std.testing.expect(hits > 0 and hits <= 1024);
    cache.deinit();
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}

test "public graph metric borrowed wire range refill preserves source and duplicate policy without allocation" {
    const a = std.testing.allocator;
    var counted: std.testing.FailingAllocator = .init(a, .{});
    var cache = ValidatedRanges.init(counted.allocator());
    defer cache.deinit();
    const raw = "[123,456]";
    try cache.remember(raw[1..4]);
    const visited = cache.validated(raw, false).?;
    const all = cache.validated(raw, true).?;
    const allocations = counted.allocations;
    // Model a collision/eviction, then refill a fully scanned fallback.
    @memset(cache.entries, .{});
    visited.refill(raw[5..8]);
    var scanner = std.json.Scanner.initCompleteInput(a, raw[5..8]);
    defer scanner.deinit();
    _ = try scanner.peekNextTokenType();
    try std.testing.expect(try visited.skip(&scanner, false));
    try std.testing.expectEqual(.end_of_document, try scanner.next());
    @memset(cache.entries, .{});
    visited.refill(raw);
    var array = std.json.Scanner.initCompleteInput(a, raw);
    defer array.deinit();
    _ = try array.peekNextTokenType();
    try std.testing.expect(!try all.skip(&array, true));
    all.refill(raw);
    try std.testing.expect(!try visited.skip(&array, true));
    try std.testing.expect(try all.skip(&array, true));
    // Even an existing entry is unusable outside this view's pinned source.
    const narrow = cache.validated(raw[1..4], true).?;
    var foreign = std.json.Scanner.initCompleteInput(a, raw);
    defer foreign.deinit();
    _ = try foreign.peekNextTokenType();
    try std.testing.expect(!try narrow.skip(&foreign, true));
    narrow.refill(raw);
    try std.testing.expectEqual(allocations, counted.allocations);
    // Growth evicts cache entries without changing either published proof.
    cache.reserveFor(128 * 1024);
    try std.testing.expect(!try all.skip(&foreign, true));
    try cache.remember(raw);
    try std.testing.expect(!try visited.skip(&foreign, true));
    try std.testing.expect(try all.skip(&foreign, true));
}

test "public graph metric borrowed wire published certificates remain immutable across columns and eviction" {
    var counted: std.testing.FailingAllocator = .init(std.testing.allocator, .{});
    var cache = ValidatedRanges.init(counted.allocator());
    defer cache.deinit();
    const raw = "123 456";
    try std.testing.expect((try cache.publish(raw, true)) == null);
    try cache.remember(raw[0..3]);
    const first = (try cache.publish(raw[0..3], false)).?;
    try std.testing.expectEqual(@as(usize, 1), counted.allocations);
    const second = (try cache.publish(raw[4..7], true)).?;
    try std.testing.expect(first != second);
    try std.testing.expectEqual(@as(usize, 2), counted.allocations);
    cache.reserveFor(128 * 1024);
    try std.testing.expectEqualStrings("123", first.proof.source);
    try std.testing.expect(!first.proof.all_keys);
    try std.testing.expectEqualStrings("456", second.proof.source);
    try std.testing.expect(second.proof.all_keys);
    cache.deinit();
    try std.testing.expectEqual(counted.allocated_bytes, counted.freed_bytes);
}

test "public graph metric borrowed wire visited array proofs are source and selector scoped" {
    const a = std.testing.allocator;
    var cache = ValidatedRanges.init(a);
    defer cache.deinit();
    var source = std.Io.Writer.Allocating.init(a);
    defer source.deinit();
    try source.writer.writeByte('[');
    for (0..64) |i| {
        if (i != 0) try source.writer.writeByte(',');
        try source.writer.writeAll("{\"y\":1,\"ignored\":{\"a\":1,\"a\":2}}");
    }
    try source.writer.writeByte(']');
    const raw = source.written();
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const selected = try @import("json_projection_plan.zig").compile(arena.allocator(), &.{.{ .parts = &.{"y"} }});
        try cache.rememberSelected(raw, selected);
    }
    const checked = cache.validated(raw, false).?;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const wanted = try @import("json_projection_plan.zig").compile(arena.allocator(), &.{.{ .parts = &.{"y"} }});
    const stronger = try @import("json_projection_plan.zig").compile(arena.allocator(), &.{.{ .parts = &.{ "ignored", "a" } }});
    var scanner = std.json.Scanner.initCompleteInput(a, raw);
    defer scanner.deinit();
    _ = try scanner.peekNextTokenType();
    try std.testing.expect(!try checked.skip(&scanner, true));
    try std.testing.expect(!try checked.skipSelected(&scanner, stronger));
    try std.testing.expect(try checked.skipSelected(&scanner, wanted));
    try std.testing.expectEqual(.end_of_document, try scanner.next());
    var foreign = std.json.Scanner.initCompleteInput(a, raw);
    defer foreign.deinit();
    _ = try foreign.peekNextTokenType();
    const narrow = cache.validated(raw[1..], false).?;
    try std.testing.expect(!try narrow.skipSelected(&foreign, wanted));
    // A scalar/syntax-only overwrite must erase the previous scope identity.
    try cache.remember(raw);
    try std.testing.expect(!try checked.skipSelected(&foreign, wanted));
    try checked.refillSelected(raw, wanted);
    try std.testing.expect(try checked.skipSelected(&foreign, wanted));
}
