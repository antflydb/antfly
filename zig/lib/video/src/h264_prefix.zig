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

//! Bounded transport/header reads. Entropy validation belongs to decode.
const std = @import("std");
const media = @import("antfly_media");
pub const Bits = struct {
    bytes: []const u8,
    cursor: usize = 1,
    zeros: u2 = 0,
    byte: u8 = 0,
    remaining: u4 = 0,
    pub fn read(self: *Bits, count: usize) !u32 {
        var out: u32 = 0;
        for (0..count) |_| {
            if (self.remaining == 0) {
                if (self.cursor >= self.bytes.len) return error.MalformedVideoPacket;
                var byte = self.bytes[self.cursor];
                self.cursor += 1;
                if (self.zeros == 2 and byte == 3) {
                    if (self.cursor >= self.bytes.len or self.bytes[self.cursor] > 3) return error.MalformedVideoPacket;
                    byte = self.bytes[self.cursor];
                    self.cursor += 1;
                    self.zeros = 0;
                } else if (self.zeros == 2 and byte <= 2) return error.MalformedVideoPacket;
                self.zeros = if (byte == 0) @min(@as(u2, 2), self.zeros + 1) else 0;
                self.byte = byte;
                self.remaining = 8;
            }
            self.remaining -= 1;
            out = (out << 1) | ((self.byte >> @as(u3, @intCast(self.remaining))) & 1);
        }
        return out;
    }
    pub fn ue(self: *Bits) !u32 {
        var zeros: u5 = 0;
        while (try self.read(1) == 0) {
            if (zeros == 30) return error.MalformedVideoPacket;
            zeros += 1;
        }
        return (@as(u32, 1) << zeros) - 1 + try self.read(zeros);
    }
    pub fn se(self: *Bits) !i32 {
        const n = try self.ue();
        return if (n & 1 != 0) @intCast((n + 1) / 2) else -@as(i32, @intCast(n / 2));
    }
};

/// Re-encode only each NAL's needed prefix. Original packet offsets/lengths
/// remain authoritative; this buffer must never be submitted to a decoder.
pub fn packet(a: std.mem.Allocator, reader: *media.mp4.Reader, index: usize, maximum: usize) ![]u8 {
    const entry = reader.packets[index];
    if (entry.size > maximum) return error.ResourceLimitExceeded;
    const n = reader.track.nal_length_bytes;
    if (n != 1 and n != 2 and n != 4) return error.MalformedVideoPacket;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var cursor: usize = 0;
    var count: usize = 0;
    while (cursor < entry.size) {
        try reader.input.control.check();
        if (entry.size - cursor <= n) return error.MalformedVideoPacket;
        // One bounded read normally covers length and complete header. Skip
        // the entropy tail using its declared length, without fetching it.
        const offset = try std.math.add(u64, entry.offset, cursor);
        var lease = try reader.input.read(offset, @min(entry.size - cursor, @as(usize, n) + 128));
        defer lease.deinit();
        var size: usize = 0;
        for (lease.bytes[0..n]) |byte| size = (size << 8) | byte;
        if (size == 0 or size > entry.size - cursor - n or lease.bytes[n] & 128 != 0) return error.MalformedVideoPacket;
        const kind = lease.bytes[n] & 31;
        if (kind == 13 or kind == 15) return error.UnsupportedDynamicVideoConfig;
        if (kind == 0 or kind >= 16) return error.UnsupportedVideoNal;
        count += 1;
        if (count > 4096) return error.ResourceLimitExceeded;
        // Parameter sets need their complete bounded syntax; slice picture
        // identity fits within 128 escaped bytes (bounded UE and POC fields).
        const retained = if (kind == 7 or kind == 8) size else @min(size, 128);
        if (retained > std.math.maxInt(u16)) return error.ResourceLimitExceeded;
        const old = out.items.len;
        const needed = try std.math.add(usize, old, @as(usize, n) + retained);
        if (needed > maximum) return error.ResourceLimitExceeded;
        try out.ensureTotalCapacity(a, needed);
        const encoded = retained;
        for (0..n) |i| {
            out.appendAssumeCapacity(@truncate(encoded >> @intCast(8 * (n - i - 1))));
        }
        const available = @min(retained, lease.bytes.len - n);
        out.appendSliceAssumeCapacity(lease.bytes[n..][0..available]);
        if (available < retained) {
            var tail = try reader.input.read(try std.math.add(u64, offset, n + available), retained - available);
            defer tail.deinit();
            out.appendSliceAssumeCapacity(tail.bytes);
        }
        cursor += n + size;
    }
    if (count == 0) return error.MalformedVideoPacket;
    return out.toOwnedSlice(a);
}
