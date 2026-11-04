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

//! A bounded bridge between independently constructed replay runtimes. Pins
//! own immutable readers, never a DB, primary epoch, or provider capability.
const std = @import("std");

pub fn Handoff(comptime Snapshot: type, comptime Source: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        snapshots: std.ArrayList(*Snapshot) = .empty,
        uses: std.ArrayList(Source) = .empty,
        bytes: u64 = 0,
        segments: usize = 0,

        /// Consumes the caller's snapshot reference on every path. Reject an
        /// entire inventory before acquiring resources when it exceeds the
        /// remaining budget. No partially pinned inventory escapes an error.
        pub fn append(self: *Self, snapshot: *Snapshot, byte_limit: u64, segment_limit: usize) !bool {
            var adopted = false;
            defer if (!adopted) snapshot.release();
            if (snapshot.read_context != null or snapshot.segments.len > segment_limit -| self.segments) return false;
            var bytes: u64 = 0;
            var native_count: usize = 0;
            for (snapshot.segments) |segment| {
                const source = segment.reader.immutableSource();
                if (source.len() > byte_limit -| self.bytes -| bytes) return false;
                bytes += source.len();
                if (source == .ranges) {
                    if (source.ranges.immutable_identity == null) return false;
                    native_count += 1;
                }
            }
            // A memory-only snapshot cannot help a new native reader share a
            // durable alias. Its bytes should be released with the old DB.
            if (native_count == 0) return false;
            try self.snapshots.ensureUnusedCapacity(self.allocator, 1);
            try self.uses.ensureUnusedCapacity(self.allocator, native_count);
            const before = self.uses.items.len;
            errdefer {
                for (self.uses.items[before..]) |source| source.releaseUse();
                self.uses.shrinkRetainingCapacity(before);
            }
            for (snapshot.segments) |segment| {
                const source = segment.reader.immutableSource();
                if (source != .ranges) continue;
                if (!source.acquireUse()) return error.ImmutableSourceUnavailable;
                self.uses.appendAssumeCapacity(source);
            }
            self.snapshots.appendAssumeCapacity(snapshot);
            self.bytes += bytes;
            self.segments += snapshot.segments.len;
            adopted = true;
            return true;
        }

        pub fn deinit(self: *Self) void {
            // Uses borrow Sources owned by the snapshots. Close in this order.
            for (self.uses.items) |source| source.releaseUse();
            for (self.snapshots.items) |snapshot| snapshot.release();
            self.uses.deinit(self.allocator);
            self.snapshots.deinit(self.allocator);
            self.* = .{ .allocator = self.allocator };
        }
    };
}

const Fixture = struct {
    const Source = union(enum) {
        contiguous: []const u8,
        ranges: struct { owner: *Fixture, immutable_identity: ?[32]u8 = @splat(1) },
        pub fn len(_: Source) u64 {
            return 16;
        }
        pub fn acquireUse(self: Source) bool {
            if (self.ranges.owner.refuse) return false;
            self.ranges.owner.uses += 1;
            return true;
        }
        pub fn releaseUse(self: Source) void {
            self.ranges.owner.uses -= 1;
        }
    };
    const Reader = struct {
        source: Source,
        pub fn immutableSource(self: Reader) Source {
            return self.source;
        }
    };
    const Segment = struct { reader: Reader };
    const Snapshot = struct {
        segments: []const Segment,
        read_context: ?*anyopaque = null,
        owner: *Fixture,
        pub fn release(self: *Snapshot) void {
            // No Source callback may access a destroyed snapshot.
            std.debug.assert(self.owner.uses == 0);
            self.owner.releases += 1;
        }
    };
    uses: usize = 0,
    releases: usize = 0,
    refuse: bool = false,

    fn allocationProbe(a: std.mem.Allocator) !void {
        var fixture: Fixture = .{};
        const segments = [_]Segment{.{ .reader = .{ .source = .{ .ranges = .{ .owner = &fixture } } } }};
        var snapshot: Snapshot = .{ .segments = &segments, .owner = &fixture };
        var handoff: Handoff(Snapshot, Source) = .{ .allocator = a };
        defer handoff.deinit();
        try std.testing.expect(try handoff.append(&snapshot, 16, 1));
        try std.testing.expectEqual(@as(usize, 1), fixture.uses);
    }
};

test "immutable handoff allocation failures return every snapshot reference" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.allocationProbe, .{});
}

test "immutable handoff rejects byte and inventory bounds before pinning" {
    var fixture: Fixture = .{};
    const segments = [_]Fixture.Segment{.{ .reader = .{ .source = .{ .ranges = .{ .owner = &fixture } } } }};
    var snapshot: Fixture.Snapshot = .{ .segments = &segments, .owner = &fixture };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Source) = .{ .allocator = std.testing.allocator };
    defer handoff.deinit();
    try std.testing.expect(!try handoff.append(&snapshot, 15, 1));
    try std.testing.expect(!try handoff.append(&snapshot, 16, 0));
    try std.testing.expectEqual(@as(usize, 0), fixture.uses);
    try std.testing.expectEqual(@as(usize, 2), fixture.releases);
    try std.testing.expectEqual(@as(u64, 0), handoff.bytes);
}

test "immutable handoff shares one budget and rejects query authority" {
    var first: Fixture = .{};
    var second: Fixture = .{};
    const first_segments = [_]Fixture.Segment{.{ .reader = .{ .source = .{ .ranges = .{ .owner = &first } } } }};
    const second_segments = [_]Fixture.Segment{.{ .reader = .{ .source = .{ .ranges = .{ .owner = &second } } } }};
    var first_snapshot: Fixture.Snapshot = .{ .segments = &first_segments, .owner = &first };
    var second_snapshot: Fixture.Snapshot = .{ .segments = &second_segments, .owner = &second, .read_context = &second };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Source) = .{ .allocator = std.testing.allocator };
    defer handoff.deinit();
    try std.testing.expect(try handoff.append(&first_snapshot, 16, 2));
    try std.testing.expect(!try handoff.append(&second_snapshot, 32, 2));
    second_snapshot.read_context = null;
    try std.testing.expect(!try handoff.append(&second_snapshot, 16, 2));
    try std.testing.expectEqual(@as(usize, 1), first.uses);
    try std.testing.expectEqual(@as(usize, 0), second.uses);
    try std.testing.expectEqual(@as(usize, 1), handoff.snapshots.items.len);
    try std.testing.expectEqual(@as(u64, 16), handoff.bytes);
}

test "immutable handoff drains acquired uses before snapshots on failure and release" {
    var first: Fixture = .{};
    var second: Fixture = .{ .refuse = true };
    const segments = [_]Fixture.Segment{
        .{ .reader = .{ .source = .{ .ranges = .{ .owner = &first } } } },
        .{ .reader = .{ .source = .{ .ranges = .{ .owner = &second } } } },
    };
    var snapshot: Fixture.Snapshot = .{ .segments = &segments, .owner = &first };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Source) = .{ .allocator = std.testing.allocator };
    defer handoff.deinit();
    try std.testing.expectError(error.ImmutableSourceUnavailable, handoff.append(&snapshot, 32, 2));
    try std.testing.expectEqual(@as(usize, 0), first.uses);
    try std.testing.expectEqual(@as(usize, 0), handoff.snapshots.items.len);
    second.refuse = false;
    try std.testing.expect(try handoff.append(&snapshot, 32, 2));
    try std.testing.expectEqual(@as(usize, 1), first.uses);
    try std.testing.expectEqual(@as(u64, 32), handoff.bytes);
    handoff.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.uses);
    try std.testing.expectEqual(@as(usize, 0), second.uses);
    try std.testing.expectEqual(@as(usize, 2), first.releases);
}
