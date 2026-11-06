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

//! Safe HEVC decode starts for a sparse selection. Container sync flags are only
//! hints, so a restart needs an IRAP in the packet itself. An open-GOP CRA also
//! leads pictures it can't reconstruct alone, so it can't start a run for one of
//! its own leading pictures, whose presentation precedes it.
const std = @import("std");
const media = @import("antfly_media");

pub const Run = struct { first: usize, last: usize };

pub fn isIrap(packet: []const u8, length_bytes: u3) !bool {
    if (length_bytes == 0 or length_bytes > 4) return error.InvalidVideoPacket;
    var cursor: usize = 0;
    while (cursor < packet.len) {
        if (packet.len - cursor < length_bytes) return error.InvalidVideoPacket;
        var size: usize = 0;
        for (packet[cursor..][0..length_bytes]) |byte| size = (size << 8) | byte;
        cursor += length_bytes;
        if (size < 2 or size > packet.len - cursor) return error.InvalidVideoPacket;
        const kind = (packet[cursor] >> 1) & 0x3f;
        if (kind < 32) return kind >= 16 and kind <= 23;
        cursor += size;
    }
    return false;
}

/// Runs in decode order covering every selected packet. `selected` must be sorted
/// and unique; a selection sharing a run decodes its references once.
pub fn plan(a: std.mem.Allocator, reader: *media.mp4.Reader, selected: []const usize) ![]Run {
    const Probe = struct {
        reader: *media.mp4.Reader,
        fn irap(self: @This(), index: usize) !bool {
            try self.reader.input.control.check();
            var lease = try self.reader.readPacket(index);
            defer lease.deinit();
            return isIrap(lease.bytes, self.reader.track.nal_length_bytes);
        }
    };
    return planPackets(a, reader.packets, selected, Probe{ .reader = reader });
}

/// A picture's start is the latest IRAP at or before it whose presentation doesn't
/// follow it, so a CRA is never the start for its own leading pictures. An earlier
/// run only helps if it already reaches back that far, so overlapping runs merge
/// to the earlier start rather than extending from a start the picture can't use.
fn planPackets(a: std.mem.Allocator, packets: []const media.mp4.Packet, selected: []const usize, probe: anytype) ![]Run {
    var runs: std.ArrayList(Run) = .empty;
    errdefer runs.deinit(a);
    var irap: std.AutoHashMapUnmanaged(usize, bool) = .empty;
    defer irap.deinit(a);
    for (selected) |target| {
        if (target >= packets.len) return error.InvalidPacketIndex;
        var start: ?usize = null;
        // Without any IRAP preceding it in presentation too, this is a leading
        // picture at the start of a stream: the latest IRAP is the best available.
        var fallback: ?usize = null;
        var candidate = target + 1;
        while (candidate > 0) {
            candidate -= 1;
            if (!packets[candidate].sync) continue;
            const entry = try irap.getOrPut(a, candidate);
            if (!entry.found_existing) entry.value_ptr.* = try probe.irap(candidate);
            if (!entry.value_ptr.*) continue;
            if (fallback == null) fallback = candidate;
            if (packets[candidate].pts <= packets[target].pts) {
                start = candidate;
                break;
            }
        }
        var first = start orelse fallback orelse return error.NoRandomAccessPoint;
        while (runs.items.len != 0 and first <= runs.items[runs.items.len - 1].last + 1) {
            first = @min(first, runs.pop().?.first);
        }
        try runs.append(a, .{ .first = first, .last = target });
    }
    return runs.toOwnedSlice(a);
}

test "HEVC IRAP detection skips parameter sets and rejects malformed lengths" {
    try std.testing.expect(try isIrap(&.{ 0, 0, 0, 2, 32 << 1, 1, 0, 0, 0, 2, 19 << 1, 1 }, 4));
    try std.testing.expect(!try isIrap(&.{ 0, 0, 0, 2, 1 << 1, 1 }, 4));
    try std.testing.expect(!try isIrap(&.{ 0, 0, 0, 2, 40 << 1, 1 }, 4));
    try std.testing.expectError(error.InvalidVideoPacket, isIrap(&.{ 0, 0, 0, 9, 19 << 1, 1 }, 4));
    try std.testing.expectError(error.InvalidVideoPacket, isIrap(&.{ 0, 0 }, 4));
}

fn testPacket(pts: i64, sync: bool) media.mp4.Packet {
    return .{ .offset = 0, .size = 1, .dts = 0, .media_pts = pts, .pts = pts, .duration = 1, .sync = sync };
}
const TestProbe = struct {
    iraps: []const usize,
    fn irap(self: @This(), index: usize) !bool {
        return std.mem.indexOfScalar(usize, self.iraps, index) != null;
    }
};

test "A CRA's leading picture reaches back to the earlier IRAP" {
    const a = std.testing.allocator;
    // Decode order: IDR, trailing, CRA, RASL that presents before the CRA, trailing.
    const packets = [_]media.mp4.Packet{ testPacket(0, true), testPacket(1, false), testPacket(4, true), testPacket(3, false), testPacket(5, false) };
    const probe = TestProbe{ .iraps = &.{ 0, 2 } };
    const both = try planPackets(a, &packets, &.{ 2, 3 }, probe);
    defer a.free(both);
    try std.testing.expectEqualSlices(Run, &.{.{ .first = 0, .last = 3 }}, both);
    const leading = try planPackets(a, &packets, &.{3}, probe);
    defer a.free(leading);
    try std.testing.expectEqualSlices(Run, &.{.{ .first = 0, .last = 3 }}, leading);
    // Pictures after the CRA don't need anything before it.
    const trailing = try planPackets(a, &packets, &.{4}, probe);
    defer a.free(trailing);
    try std.testing.expectEqualSlices(Run, &.{.{ .first = 2, .last = 4 }}, trailing);
}

test "Separated selections stay in separate runs, and sync flags alone don't start one" {
    const a = std.testing.allocator;
    const packets = [_]media.mp4.Packet{ testPacket(0, true), testPacket(1, true), testPacket(2, true), testPacket(3, true), testPacket(4, true), testPacket(5, true) };
    const runs = try planPackets(a, &packets, &.{ 1, 5 }, TestProbe{ .iraps = &.{ 0, 4 } });
    defer a.free(runs);
    try std.testing.expectEqualSlices(Run, &.{ .{ .first = 0, .last = 1 }, .{ .first = 4, .last = 5 } }, runs);
    try std.testing.expectError(error.NoRandomAccessPoint, planPackets(a, &packets, &.{1}, TestProbe{ .iraps = &.{} }));
}

test "A leading picture at the start of a stream uses the only IRAP there is" {
    const a = std.testing.allocator;
    const packets = [_]media.mp4.Packet{ testPacket(4, true), testPacket(3, false) };
    const runs = try planPackets(a, &packets, &.{1}, TestProbe{ .iraps = &.{0} });
    defer a.free(runs);
    try std.testing.expectEqualSlices(Run, &.{.{ .first = 0, .last = 1 }}, runs);
}
