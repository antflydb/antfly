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

pub fn Handoff(comptime Snapshot: type, comptime Lease: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        leases: std.ArrayList(Lease) = .empty,
        bytes: u64 = 0,
        visited: usize = 0,

        /// Consumes the inventory reference on every path. Retain individual
        /// physical readers that fit, rather than rejecting a large index or
        /// keeping its entire inventory/global statistics alive. Both visits
        /// and retained ranges share one file-wide budget across indexes.
        pub fn append(self: *Self, snapshot: *Snapshot, byte_limit: u64, segment_limit: usize) !bool {
            defer snapshot.release();
            if (snapshot.read_context != null or self.bytes >= byte_limit) return false;
            const visits = @min(snapshot.segments.len, segment_limit -| self.visited);
            if (visits == 0) return false;
            try self.leases.ensureUnusedCapacity(self.allocator, visits);
            const before = self.leases.items.len;
            var position = snapshot.segments.len;
            for (0..visits) |_| {
                position -= 1;
                self.visited += 1;
                const segment = &snapshot.segments[position];
                const bytes = segment.reader.immutableSource().len();
                if (bytes > byte_limit -| self.bytes) continue;
                const lease = segment.retainImmutable() orelse continue;
                self.leases.appendAssumeCapacity(lease);
                self.bytes += bytes;
                if (self.bytes == byte_limit) break;
            }
            return self.leases.items.len != before;
        }

        pub fn deinit(self: *Self) void {
            for (self.leases.items) |*lease| lease.deinit();
            self.leases.deinit(self.allocator);
            self.* = .{ .allocator = self.allocator };
        }
    };
}

const Fixture = struct {
    const Source = struct {
        owner: *Fixture,
        bytes: u64 = 16,
        pub fn len(self: Source) u64 {
            return self.bytes;
        }
    };
    const Reader = struct {
        source: Source,
        pub fn immutableSource(self: Reader) Source {
            return self.source;
        }
    };
    const Lease = struct {
        owner: *Fixture,
        pub fn deinit(self: *Lease) void {
            std.debug.assert(self.owner.uses > 0 and self.owner.references > 0);
            self.owner.uses -= 1;
            self.owner.references -= 1;
        }
    };
    const Segment = struct {
        reader: Reader,
        pub fn retainImmutable(self: *const Segment) ?Lease {
            const owner = self.reader.source.owner;
            if (owner.refuse) return null;
            owner.uses += 1;
            owner.references += 1;
            return .{ .owner = owner };
        }
    };
    const Snapshot = struct {
        segments: []const Segment,
        read_context: ?*anyopaque = null,
        owner: *Fixture,
        pub fn release(self: *Snapshot) void {
            self.owner.releases += 1;
        }
    };
    uses: usize = 0,
    references: usize = 0,
    releases: usize = 0,
    refuse: bool = false,

    fn allocationProbe(a: std.mem.Allocator) !void {
        var fixture: Fixture = .{};
        const segments = [_]Segment{.{ .reader = .{ .source = .{ .owner = &fixture } } }};
        var snapshot: Snapshot = .{ .segments = &segments, .owner = &fixture };
        var handoff: Handoff(Snapshot, Lease) = .{ .allocator = a };
        defer handoff.deinit();
        try std.testing.expect(try handoff.append(&snapshot, 16, 1));
        try std.testing.expectEqual(@as(usize, 1), fixture.uses);
        // The inventory is already gone; only individual reader refs remain.
        try std.testing.expectEqual(@as(usize, 1), fixture.releases);
        try std.testing.expectEqual(@as(usize, 1), fixture.references);
    }
};

test "immutable handoff allocation failures return every inventory reference" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.allocationProbe, .{});
}

test "immutable handoff rejects byte and visit bounds before pinning" {
    var fixture: Fixture = .{};
    const segments = [_]Fixture.Segment{.{ .reader = .{ .source = .{ .owner = &fixture } } }};
    var snapshot: Fixture.Snapshot = .{ .segments = &segments, .owner = &fixture };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Lease) = .{ .allocator = std.testing.allocator };
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
    const first_segments = [_]Fixture.Segment{.{ .reader = .{ .source = .{ .owner = &first } } }};
    const second_segments = [_]Fixture.Segment{.{ .reader = .{ .source = .{ .owner = &second } } }};
    var first_snapshot: Fixture.Snapshot = .{ .segments = &first_segments, .owner = &first };
    var second_snapshot: Fixture.Snapshot = .{ .segments = &second_segments, .owner = &second, .read_context = &second };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Lease) = .{ .allocator = std.testing.allocator };
    defer handoff.deinit();
    try std.testing.expect(try handoff.append(&first_snapshot, 16, 2));
    try std.testing.expect(!try handoff.append(&second_snapshot, 32, 2));
    second_snapshot.read_context = null;
    try std.testing.expect(!try handoff.append(&second_snapshot, 16, 2));
    try std.testing.expectEqual(@as(usize, 1), first.uses);
    try std.testing.expectEqual(@as(usize, 0), second.uses);
    try std.testing.expectEqual(@as(usize, 1), handoff.leases.items.len);
    try std.testing.expectEqual(@as(u64, 16), handoff.bytes);
}

test "immutable handoff skips unavailable and oversized readers without retaining their inventory" {
    var first: Fixture = .{};
    var second: Fixture = .{ .refuse = true };
    const segments = [_]Fixture.Segment{
        .{ .reader = .{ .source = .{ .owner = &first } } },
        .{ .reader = .{ .source = .{ .owner = &second } } },
        .{ .reader = .{ .source = .{ .owner = &first, .bytes = 800 * 1024 * 1024 } } },
    };
    var snapshot: Fixture.Snapshot = .{ .segments = &segments, .owner = &first };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Lease) = .{ .allocator = std.testing.allocator };
    defer handoff.deinit();
    try std.testing.expect(try handoff.append(&snapshot, 16, 3));
    try std.testing.expectEqual(@as(usize, 1), first.uses);
    try std.testing.expectEqual(@as(usize, 0), second.uses);
    try std.testing.expectEqual(@as(usize, 1), first.releases);
    try std.testing.expectEqual(@as(usize, 1), handoff.leases.items.len);
    try std.testing.expectEqual(@as(u64, 16), handoff.bytes);
    handoff.deinit();
    try std.testing.expectEqual(@as(usize, 0), first.uses);
    try std.testing.expectEqual(@as(usize, 0), first.references);
}

test "immutable handoff bounds visits even when every range exceeds the budget" {
    var fixture: Fixture = .{};
    const segments: [128]Fixture.Segment = @splat(.{ .reader = .{ .source = .{ .owner = &fixture, .bytes = 800 * 1024 * 1024 } } });
    var snapshot: Fixture.Snapshot = .{ .segments = &segments, .owner = &fixture };
    var handoff: Handoff(Fixture.Snapshot, Fixture.Lease) = .{ .allocator = std.testing.allocator };
    defer handoff.deinit();
    try std.testing.expect(!try handoff.append(&snapshot, 16, 4));
    try std.testing.expect(!try handoff.append(&snapshot, 16, 4));
    try std.testing.expectEqual(@as(usize, 4), handoff.visited);
    try std.testing.expectEqual(@as(usize, 0), fixture.references);
    try std.testing.expectEqual(@as(usize, 2), fixture.releases);
}
