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

//! Union of visible unit-scope identities, including pre-inventory raw tails
//! and head-only generations. Canonical inventory keys order all three sources
//! identically. Advancing seeks past a whole scope, never walks its raw chunks.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const generations = @import("artifact_chunk_generation.zig");
const publication = @import("artifact_publication.zig");
const Entry = @import("../backend_erased.zig").Entry;

pub const Scope = struct {
    /// Borrows the cursor until its next peek/close; always an inventory key.
    key: []const u8,
    /// Encoded component including its two-byte terminator.
    unit: []const u8,
    bytes: usize,
};

pub fn Cursor(comptime Txn: type) type {
    return struct {
        const Self = @This();
        alloc: std.mem.Allocator,
        prefixes: [3][]u8 = .{ &.{}, &.{}, &.{} },
        cursors: [3]?Txn.CursorAdapter = .{ null, null, null },
        rows: [3]?Entry = .{ null, null, null },
        current: std.ArrayListUnmanaged(u8) = .empty,
        seek: std.ArrayListUnmanaged(u8) = .empty,

        pub fn open(alloc: std.mem.Allocator, txn: *Txn, document: []const u8, producer: []const u8, after: []const u8) !Self {
            var self: Self = .{ .alloc = alloc };
            errdefer self.close();
            const root = try chunks.keyAlloc(alloc, document, producer);
            defer alloc.free(root);
            self.prefixes[0] = try std.mem.concat(alloc, u8, &.{ root, &.{keys.document_unit_record_kind} });
            self.prefixes[1] = try alloc.dupe(u8, self.prefixes[0]);
            self.prefixes[1][keys.findComponentTerminator(root, 1).? + 2] = keys.producer_generation_head_kind;
            const raw_root = try keys.artifactNamedPrefixAlloc(alloc, document, "chunk", producer);
            defer alloc.free(raw_root);
            self.prefixes[2] = try std.mem.concat(alloc, u8, &.{ raw_root, &.{keys.document_unit_record_kind} });
            if (after.len > 1024 * 1024 or (after.len != 0 and
                (!std.mem.startsWith(u8, after, self.prefixes[0]) or !chunks.isKey(after)))) return error.InvalidBatchRequest;
            for (0..3) |i| {
                self.cursors[i] = try txn.openPhysicalCursorAdapter();
                if (after.len == 0) {
                    self.rows[i] = self.inRange(i, try self.cursors[i].?.seekAtOrAfter(self.prefixes[i]));
                } else try self.skip(i, after[self.prefixes[0].len..]);
            }
            return self;
        }

        pub fn close(self: *Self) void {
            for (&self.cursors) |*cursor| if (cursor.*) |*value| value.close();
            for (self.prefixes) |prefix| self.alloc.free(prefix);
            self.current.deinit(self.alloc);
            self.seek.deinit(self.alloc);
            self.* = undefined;
        }

        fn inRange(self: *const Self, i: usize, row: ?Entry) ?Entry {
            const found = row orelse return null;
            return if (std.mem.startsWith(u8, found.key, self.prefixes[i])) found else null;
        }

        fn unit(self: *const Self, i: usize, row: Entry) ![]const u8 {
            if (row.key.len > 1024 * 1024) return error.ResourceBudgetExceeded;
            const suffix = if (i == 2) blk: {
                if (!keys.isChunkArtifactRecordKey(row.key)) return error.ArtifactCatalogCorrupt;
                // Physical cursors do not resolve an external payload. This
                // is at most one stored entry per scope, not a payload walk.
                if (row.value.len > publication.max_payload_bytes) return error.ResourceBudgetExceeded;
                break :blk row.key[self.prefixes[i].len .. row.key.len - 5];
            } else blk: {
                if (!chunks.isScopeKey(row.key, if (i == 0) keys.producer_stream_manifest_kind else keys.producer_generation_head_kind)) return error.ArtifactCatalogCorrupt;
                break :blk row.key[self.prefixes[i].len..];
            };
            if (i == 0) {
                _ = try chunks.Manifest.decode(row.value);
            } else if (i == 1) {
                const spec = try generations.Spec.decode(row.value);
                var hash = std.crypto.hash.sha2.Sha256.init(.{});
                hash.update(self.prefixes[0]);
                hash.update(suffix);
                if (!std.meta.eql(spec.scope_digest, hash.finalResult())) return error.ArtifactCatalogCorrupt;
            }
            return suffix;
        }

        pub fn peek(self: *Self) !?Scope {
            var first: ?[]const u8 = null;
            var suffixes: [3]?[]const u8 = .{ null, null, null };
            for (self.rows, 0..) |row, i| if (row) |found| {
                const suffix = try self.unit(i, found);
                suffixes[i] = suffix;
                if (first == null or std.mem.order(u8, suffix, first.?) == .lt) first = suffix;
            };
            const suffix = first orelse return null;
            self.current.clearRetainingCapacity();
            try self.current.appendSlice(self.alloc, self.prefixes[0]);
            try self.current.appendSlice(self.alloc, suffix);
            if (self.current.items.len > 1024 * 1024) return error.ResourceBudgetExceeded;
            var bytes: usize = 0;
            for (suffixes, 0..) |candidate, i| if (candidate) |value| {
                if (std.mem.eql(u8, value, suffix)) bytes +|= self.rows[i].?.key.len +| self.rows[i].?.value.len;
            };
            return .{ .key = self.current.items, .unit = self.current.items[self.prefixes[0].len..], .bytes = bytes };
        }

        fn skip(self: *Self, i: usize, suffix: []const u8) !void {
            if (suffix.len < 3 or !std.mem.eql(u8, suffix[suffix.len - 2 ..], &.{ 0, 0 })) return error.ArtifactCatalogCorrupt;
            self.seek.clearRetainingCapacity();
            try self.seek.appendSlice(self.alloc, self.prefixes[i]);
            try self.seek.appendSlice(self.alloc, suffix);
            // Every encoded component ends in 00 00. Its prefix successor
            // excludes all chunk ordinals, including a sparse enormous tail.
            self.seek.items[self.seek.items.len - 1] = 1;
            self.rows[i] = self.inRange(i, try self.cursors[i].?.seekAtOrAfter(self.seek.items));
        }

        pub fn advance(self: *Self) !void {
            if (self.current.items.len == 0) return error.InvalidBatchRequest;
            const suffix = self.current.items[self.prefixes[0].len..];
            for (0..3) |i| if (self.rows[i]) |row| {
                if (std.mem.eql(u8, suffix, try self.unit(i, row))) try self.skip(i, suffix);
            };
            self.current.clearRetainingCapacity();
        }
    };
}

test "ordered artifact inventory unit scope union deduplicates heads and seeks past raw tails" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var rows: std.ArrayList(Entry) = .empty;
    const empty = chunks.Builder.init().finish().encode();
    const first = "\x00first";
    for ([_]u32{ 0, 1, 1000000000 }) |ordinal| try rows.append(owned, .{ .key = try keys.documentUnitChunkArtifactKeyAlloc(owned, "doc", "child", first, ordinal), .value = "opaque stored payload; never decoded" });
    try rows.append(owned, .{ .key = try keys.documentUnitChunkArtifactKeyAlloc(owned, "doc", "child", "a", 0), .value = "opaque" });
    for ([_][]const u8{ "a", "z\xff" }) |id| try rows.append(owned, .{ .key = try chunks.scopedKeyAlloc(owned, "doc", "child", id), .value = &empty });
    for ([_][]const u8{ "a", "a\x00" }) |id| {
        const scope = try chunks.scopedKeyAlloc(owned, "doc", "child", id);
        const spec = try generations.Spec.init(.{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) }, scope, @splat(3), chunks.Builder.init().finish(), 1);
        scope[keys.findComponentTerminator(scope, 1).? + 2] = keys.producer_generation_head_kind;
        try rows.append(owned, .{ .key = scope, .value = try owned.dupe(u8, &spec.encode()) });
    }
    try rows.append(owned, .{ .key = try keys.chunkArtifactKeyAlloc(owned, "doc", "child", 0), .value = "root is not a unit" });
    try rows.append(owned, .{ .key = try keys.documentUnitChunkArtifactKeyAlloc(owned, "doc", "other", "outside", 0), .value = "another producer" });
    const Order = struct {
        fn less(_: void, a: Entry, b: Entry) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    };
    std.mem.sort(Entry, rows.items, {}, Order.less);
    const Store = struct {
        const Self = @This();
        rows: []const Entry,
        opened: usize = 0,
        closed: usize = 0,
        seeks: usize = 0,
        // No next() or get(): discovery can neither walk a tail nor load
        // logical unit/chunk payloads through a separate read API.
        pub const CursorAdapter = struct {
            store: *Self,
            pub fn close(self: *@This()) void {
                self.store.closed += 1;
            }
            pub fn seekAtOrAfter(self: *@This(), lower: []const u8) !?Entry {
                self.store.seeks += 1;
                for (self.store.rows) |row| if (std.mem.order(u8, row.key, lower) != .lt) return row;
                return null;
            }
        };
        pub fn openPhysicalCursorAdapter(self: *@This()) !CursorAdapter {
            self.opened += 1;
            return .{ .store = self };
        }
    };
    var store: Store = .{ .rows = rows.items };
    {
        var cursor = try Cursor(Store).open(alloc, &store, "doc", "child", "");
        defer cursor.close();
        for ([_][]const u8{ first, "a", "a\x00", "z\xff" }) |expected| {
            const scope = (try cursor.peek()).?;
            const decoded = try keys.decodeBodyAlloc(alloc, scope.unit[0 .. scope.unit.len - 2]);
            defer alloc.free(decoded);
            try std.testing.expectEqualStrings(expected, decoded);
            try std.testing.expect(chunks.isKey(scope.key));
            try cursor.advance();
        }
        try std.testing.expect((try cursor.peek()) == null);
        try std.testing.expectEqual(@as(usize, 9), store.seeks);
    }
    try std.testing.expectEqual(store.opened, store.closed);
    const after = try chunks.scopedKeyAlloc(owned, "doc", "child", first);
    {
        var cursor = try Cursor(Store).open(alloc, &store, "doc", "child", after);
        defer cursor.close();
        const next = (try cursor.peek()).?;
        try std.testing.expectEqualSlices(u8, "a\x00\x00", next.unit);
    }
    const foreign = try chunks.scopedKeyAlloc(owned, "doc", "other", first);
    try std.testing.expectError(error.InvalidBatchRequest, Cursor(Store).open(alloc, &store, "doc", "child", foreign));
    const AllocationCheck = struct {
        fn run(a: std.mem.Allocator, entries: []const Entry) !void {
            var mock: Store = .{ .rows = entries };
            defer std.debug.assert(mock.opened == mock.closed);
            var cursor = try Cursor(Store).open(a, &mock, "doc", "child", "");
            defer cursor.close();
            while (try cursor.peek() != null) try cursor.advance();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationCheck.run, .{rows.items});
}
