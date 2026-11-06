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
    var runs: std.ArrayList(Run) = .empty;
    errdefer runs.deinit(a);
    for (selected) |target| {
        if (target >= reader.packets.len) return error.InvalidPacketIndex;
        if (runs.items.len != 0 and target <= runs.items[runs.items.len - 1].last) continue;
        const floor = if (runs.items.len != 0) runs.items[runs.items.len - 1].last + 1 else 0;
        var start: ?usize = null;
        var candidate = target + 1;
        while (candidate > floor) {
            candidate -= 1;
            const packet = reader.packets[candidate];
            if (!packet.sync or packet.pts > reader.packets[target].pts) continue;
            try reader.input.control.check();
            var lease = try reader.readPacket(candidate);
            defer lease.deinit();
            if (try isIrap(lease.bytes, reader.track.nal_length_bytes)) {
                start = candidate;
                break;
            }
        }
        if (start) |first| {
            // Replaying a whole GOP costs less than reopening a session when
            // the new start already follows the previous run directly.
            if (first == floor and runs.items.len != 0) {
                runs.items[runs.items.len - 1].last = target;
            } else {
                try runs.append(a, .{ .first = first, .last = target });
            }
        } else if (runs.items.len != 0) {
            // No restart since the previous run, so its references carry forward.
            runs.items[runs.items.len - 1].last = target;
        } else {
            return error.NoRandomAccessPoint;
        }
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
