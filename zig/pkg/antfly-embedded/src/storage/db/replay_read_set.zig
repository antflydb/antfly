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

//! Prepared replay inputs. Values are fingerprints, so large extraction
//! payloads are not retained for a second time while publication waits.
const std = @import("std");
const Hash = std.crypto.hash.Blake3;
pub const Point = struct { key: []const u8, digest: ?[32]u8 };
pub const Range = struct { lower: []const u8, upper: []const u8, digest: [32]u8 };
pub const View = struct {
    points: []const Point = &.{},
    ranges: []const Range = &.{},

    /// The caller holds the live writer fence for this entire validation and
    /// the following commit. Use logical reads, matching the resolver's store.
    pub fn validate(self: View, txn: anytype) !void {
        for (self.points) |point| {
            const value = txn.get(point.key) catch |err| if (err == error.NotFound) null else return err;
            if (!std.meta.eql(point.digest, fingerprint(value))) return error.EnrichmentWaitCanceled;
        }
        for (self.ranges) |range| {
            var cursor = try txn.openCursor();
            defer cursor.close();
            cursor.setUpperBound(range.upper);
            var hash = Hash.init(.{});
            var row = try cursor.seekAtOrAfter(range.lower);
            while (row) |entry| {
                hashEntry(&hash, entry.key, entry.value);
                row = try cursor.next();
            }
            if (!std.mem.eql(u8, &range.digest, &finish(&hash))) return error.EnrichmentWaitCanceled;
        }
    }
};

pub fn finish(hash: *Hash) [32]u8 {
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

pub fn fingerprint(value: ?[]const u8) ?[32]u8 {
    const bytes = value orelse return null;
    var digest: [32]u8 = undefined;
    Hash.hash(bytes, &digest, .{});
    return digest;
}

pub fn hashEntry(hash: *Hash, key: []const u8, value: []const u8) void {
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, key.len, .little);
    hash.update(&size);
    hash.update(key);
    std.mem.writeInt(u64, &size, value.len, .little);
    hash.update(&size);
    hash.update(value);
}

pub const Capture = struct {
    arena: std.heap.ArenaAllocator,
    points: std.ArrayList(Point) = .empty,
    ranges: std.ArrayList(Range) = .empty,
    seen: std.StringHashMapUnmanaged(usize) = .empty,
    ranges_seen: std.StringHashMapUnmanaged(usize) = .empty,

    pub fn init(alloc: std.mem.Allocator) Capture {
        return .{ .arena = .init(alloc) };
    }
    pub fn deinit(self: *Capture) void {
        self.arena.deinit();
    }
    pub fn view(self: *const Capture) View {
        return .{ .points = self.points.items, .ranges = self.ranges.items };
    }
    pub fn notePoint(self: *Capture, key: []const u8, value: ?[]const u8) !void {
        const digest = fingerprint(value);
        if (self.seen.get(key)) |index| {
            if (!std.meta.eql(self.points.items[index].digest, digest)) return error.EnrichmentWaitCanceled;
            return;
        }
        const a = self.arena.allocator();
        const owned = try a.dupe(u8, key);
        try self.seen.put(a, owned, self.points.items.len);
        try self.points.append(a, .{ .key = owned, .digest = digest });
    }
    pub fn noteRange(self: *Capture, lower: []const u8, upper: []const u8, digest: [32]u8) !void {
        const a = self.arena.allocator();
        // Repeated mentions often search the same candidate prefix. Keep one
        // validation for identical bounds, while rejecting inconsistent reads.
        const bounds = try a.alloc(u8, 8 + lower.len + upper.len);
        std.mem.writeInt(u64, bounds[0..8], lower.len, .little);
        @memcpy(bounds[8..][0..lower.len], lower);
        @memcpy(bounds[8 + lower.len ..], upper);
        if (self.ranges_seen.get(bounds)) |index| {
            a.free(bounds);
            if (!std.mem.eql(u8, &self.ranges.items[index].digest, &digest)) return error.EnrichmentWaitCanceled;
            return;
        }
        try self.ranges_seen.put(a, bounds, self.ranges.items.len);
        try self.ranges.append(a, .{ .lower = bounds[8..][0..lower.len], .upper = bounds[8 + lower.len ..], .digest = digest });
    }
};
